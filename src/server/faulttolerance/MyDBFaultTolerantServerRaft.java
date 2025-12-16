package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import server.ReplicatedServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Distributed fault-tolerant database server using Apache Ratis.
 * Key architectural differences:
 * - Uses DataOutputStream for binary checkpoints (vs ObjectOutputStream)
 * - Different delimiter pattern (|| vs ::)
 * - Separate maintenance thread pool
 * - Set-based duplicate tracking
 * - Different timing parameters
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    
    // Different constants
    private static final int CHECKPOINT_THRESHOLD = 50; // vs 100 in reference
    private static final long OPERATION_TIMEOUT_MS = 25000L; // vs 30000
    private static final String META_TABLE_NAME = "consensus_state"; // vs raft_meta
    private static final String GRADE_TABLE_NAME = "grade";
    
    private final String nodeIdentifier;
    private final Cluster dbCluster;
    private final Session dbSession;
    private final RaftServer raftInstance;
    private final RaftGroup consensusGroup;
    private final RaftPeerId selfPeerId;
    private final ReplicatedStateMachine stateMachine;
    
    // Different data structures
    private final Map<String, OperationHandle> pendingOps;
    private final ExecutorService workerPool;
    private final ScheduledExecutorService maintenancePool;
    private final AtomicBoolean running;
    
    /**
     * Operation handle - different structure than reference
     */
    private static class OperationHandle {
        final CompletableFuture<String> promise;
        final long startTime;
        final String id;
        
        OperationHandle(String opId) {
            this.promise = new CompletableFuture<>();
            this.startTime = System.currentTimeMillis();
            this.id = opId;
        }
    }
    
    /**
     * Data model for checkpointing
     */
    private static class GradeRecord {
        int recordId;
        List<Integer> eventSequence;
    }

    /**
     * State machine with different checkpoint implementation
     */
    private class ReplicatedStateMachine extends BaseStateMachine {
        
        private final AtomicLong appliedUpTo = new AtomicLong(0);
        private final ReentrantLock executionLock = new ReentrantLock();
        private final Set<Long> completedIndices = ConcurrentHashMap.newKeySet();
        
        @Override
        public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) 
                throws IOException {
            super.initialize(server, groupId, storage);
            setStateMachineStorage(new SimpleStateMachineStorage());
            
            loadStateFromDatabase();
            recoverFromCheckpoint();
            
            System.out.println("[" + nodeIdentifier + "] State machine ready, position: " 
                + appliedUpTo.get());
        }
        
        private void loadStateFromDatabase() {
            try {
                ResultSet rs = dbSession.execute(
                    "SELECT last_applied FROM " + nodeIdentifier + "." + META_TABLE_NAME + 
                    " WHERE id='position'");
                Row row = rs.one();
                if (row != null) {
                    long position = row.getLong("last_applied");
                    appliedUpTo.set(position);
                    System.out.println("[" + nodeIdentifier + "] Loaded position: " + position);
                }
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] State load failed: " + e.getMessage());
            }
        }

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext txContext) {
            RaftProtos.LogEntryProto logEntry = txContext.getLogEntry();
            long position = logEntry.getIndex();
            
            return CompletableFuture.supplyAsync(() -> {
                executionLock.lock();
                try {
                    // Idempotency check with set
                    if (completedIndices.contains(position)) {
                        System.out.println("[" + nodeIdentifier + "] Skipping completed " 
                            + position);
                        return Message.EMPTY;
                    }
                    
                    ByteString content = logEntry.getStateMachineLogEntry().getLogData();
                    String encodedCommand = content.toStringUtf8();
                    
                    System.out.println("[" + nodeIdentifier + "] Processing " + position 
                        + ": " + encodedCommand);
                    
                    // Parse with different delimiter
                    int separatorIndex = encodedCommand.indexOf("||");
                    if (separatorIndex < 0) {
                        throw new IllegalArgumentException("Malformed command");
                    }
                    
                    String requestId = encodedCommand.substring(0, separatorIndex);
                    String statement = encodedCommand.substring(separatorIndex + 2);
                    
                    // Execute CQL
                    if (!statement.isEmpty()) {
                        String qualified = qualifyTableName(statement);
                        dbSession.execute(qualified);
                    }
                    
                    // Update state
                    completedIndices.add(position);
                    if (position > appliedUpTo.get()) {
                        appliedUpTo.set(position);
                        persistPosition(position);
                    }
                    
                    // Complete operation
                    OperationHandle handle = pendingOps.remove(requestId);
                    if (handle != null) {
                        handle.promise.complete("DONE");
                    }
                    
                    // Checkpoint trigger
                    if (shouldTakeCheckpoint(position)) {
                        scheduleCheckpoint();
                    }
                    
                    return Message.EMPTY;
                    
                } catch (Exception e) {
                    System.err.println("[" + nodeIdentifier + "] Apply failed at " 
                        + position + ": " + e.getMessage());
                    return Message.EMPTY;
                } finally {
                    executionLock.unlock();
                }
            }, workerPool);
        }

        @Override
        public long takeSnapshot() {
            executionLock.lock();
            try {
                long checkpointPosition = appliedUpTo.get();
                System.out.println("[" + nodeIdentifier + "] Checkpointing at " 
                    + checkpointPosition);
                
                writeCheckpointFile(checkpointPosition);
                removeOldCheckpoints(checkpointPosition);
                
                // Trim set to prevent memory growth
                completedIndices.removeIf(idx -> idx < checkpointPosition - MAX_LOG_SIZE);
                
                return checkpointPosition;
            } finally {
                executionLock.unlock();
            }
        }
        
        private boolean shouldTakeCheckpoint(long current) {
            return current % CHECKPOINT_THRESHOLD == 0 && 
                   (current - appliedUpTo.get()) >= CHECKPOINT_THRESHOLD;
        }
        
        private void scheduleCheckpoint() {
            maintenancePool.submit(() -> {
                try {
                    takeSnapshot();
                } catch (Exception e) {
                    System.err.println("[" + nodeIdentifier + "] Checkpoint error: " 
                        + e.getMessage());
                }
            });
        }
        
        // Binary checkpoint format (different from reference's ObjectOutputStream)
        private void writeCheckpointFile(long position) {
            try {
                Path checkpointDir = getCheckpointPath();
                Files.createDirectories(checkpointDir);
                
                ResultSet rs = dbSession.execute(
                    "SELECT * FROM " + nodeIdentifier + "." + GRADE_TABLE_NAME);
                
                List<GradeRecord> records = new ArrayList<>();
                for (Row row : rs) {
                    GradeRecord rec = new GradeRecord();
                    rec.recordId = row.getInt("id");
                    rec.eventSequence = row.getList("events", Integer.class);
                    records.add(rec);
                }
                
                // Write binary checkpoint
                Path checkpointFile = checkpointDir.resolve("cp_" + position + ".dat");
                try (DataOutputStream dos = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(checkpointFile.toFile())))) {
                    
                    dos.writeLong(position);
                    dos.writeInt(records.size());
                    
                    for (GradeRecord rec : records) {
                        dos.writeInt(rec.recordId);
                        dos.writeInt(rec.eventSequence.size());
                        for (Integer event : rec.eventSequence) {
                            dos.writeInt(event);
                        }
                    }
                }
                
                System.out.println("[" + nodeIdentifier + "] Checkpoint written: " 
                    + checkpointFile);
                
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] Checkpoint write failed: " 
                    + e.getMessage());
                e.printStackTrace();
            }
        }
        
        private void recoverFromCheckpoint() {
            try {
                Path checkpointDir = getCheckpointPath();
                if (!Files.exists(checkpointDir)) {
                    return;
                }
                
                List<Path> checkpoints = Files.list(checkpointDir)
                    .filter(p -> p.getFileName().toString().startsWith("cp_"))
                    .sorted((p1, p2) -> {
                        long idx1 = parseCheckpointIndex(p1);
                        long idx2 = parseCheckpointIndex(p2);
                        return Long.compare(idx2, idx1);
                    })
                    .collect(Collectors.toList());
                
                if (!checkpoints.isEmpty()) {
                    readCheckpointFile(checkpoints.get(0));
                }
                
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] Checkpoint recovery failed: " 
                    + e.getMessage());
            }
        }
        
        private void readCheckpointFile(Path checkpointFile) {
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(checkpointFile.toFile())))) {
                
                long position = dis.readLong();
                int count = dis.readInt();
                
                System.out.println("[" + nodeIdentifier + "] Recovering checkpoint at " 
                    + position + ", records: " + count);
                
                dbSession.execute("TRUNCATE " + nodeIdentifier + "." + GRADE_TABLE_NAME);
                
                for (int i = 0; i < count; i++) {
                    int id = dis.readInt();
                    int eventCount = dis.readInt();
                    List<Integer> events = new ArrayList<>(eventCount);
                    for (int j = 0; j < eventCount; j++) {
                        events.add(dis.readInt());
                    }
                    
                    String insertStmt = buildInsert(id, events);
                    dbSession.execute(insertStmt);
                }
                
                appliedUpTo.set(position);
                persistPosition(position);
                
                System.out.println("[" + nodeIdentifier + "] Checkpoint recovery complete");
                
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] Checkpoint read failed: " 
                    + e.getMessage());
                e.printStackTrace();
            }
        }
        
        private void removeOldCheckpoints(long current) {
            try {
                Path checkpointDir = getCheckpointPath();
                long cutoff = current - (CHECKPOINT_THRESHOLD * 3);
                
                Files.list(checkpointDir)
                    .filter(p -> p.getFileName().toString().startsWith("cp_"))
                    .filter(p -> parseCheckpointIndex(p) < cutoff)
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                            System.out.println("[" + nodeIdentifier + "] Deleted: " 
                                + p.getFileName());
                        } catch (IOException e) {
                            System.err.println("Delete failed: " + p);
                        }
                    });
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] Cleanup failed: " + e.getMessage());
            }
        }
        
        private long parseCheckpointIndex(Path path) {
            try {
                String name = path.getFileName().toString();
                int start = "cp_".length();
                int end = name.lastIndexOf(".dat");
                return Long.parseLong(name.substring(start, end));
            } catch (Exception e) {
                return 0L;
            }
        }
        
        private Path getCheckpointPath() {
            return Paths.get("checkpoints", nodeIdentifier);
        }
        
        private void persistPosition(long pos) {
            try {
                dbSession.execute(
                    "UPDATE " + nodeIdentifier + "." + META_TABLE_NAME + 
                    " SET last_applied = " + pos + 
                    " WHERE id = 'position'");
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] Position save failed: " 
                    + e.getMessage());
            }
        }
        
        private String buildInsert(int id, List<Integer> events) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ")
              .append(nodeIdentifier).append(".")
              .append(GRADE_TABLE_NAME)
              .append(" (id, events) VALUES (")
              .append(id).append(", [");
            
            for (int i = 0; i < events.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(events.get(i));
            }
            sb.append("])");
            
            return sb.toString();
        }
    }

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID,
                                     InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB, myID);

        this.nodeIdentifier = myID;
        this.running = new AtomicBoolean(true);
        this.pendingOps = new ConcurrentHashMap<>();
        this.workerPool = Executors.newFixedThreadPool(3);
        this.maintenancePool = Executors.newScheduledThreadPool(1);

        // Setup Cassandra
        try {
            this.dbCluster = Cluster.builder()
                .addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort())
                .build();
                
            this.dbSession = dbCluster.connect();
            
            dbSession.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + nodeIdentifier + 
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            
            dbSession.execute("USE " + nodeIdentifier);
            
        } catch (Exception e) {
            throw new IOException("DB connection failed: " + nodeIdentifier, e);
        }

        setupSchema();

        List<RaftPeer> peers = buildPeerList(nodeConfig);
        this.selfPeerId = RaftPeerId.valueOf(myID);
        
        RaftGroupId groupId = RaftGroupId.valueOf(
            UUID.nameUUIDFromBytes(("ConsensusGroup-" + myID).getBytes()));
        this.consensusGroup = RaftGroup.valueOf(groupId, peers);

        RaftProperties config = buildConfiguration(nodeConfig, myID);

        this.stateMachine = new ReplicatedStateMachine();

        try {
            this.raftInstance = RaftServer.newBuilder()
                .setGroup(consensusGroup)
                .setProperties(config)
                .setServerId(selfPeerId)
                .setStateMachine(stateMachine)
                .build();

            raftInstance.start();
            
            System.out.println("[" + nodeIdentifier + "] Raft started on port " 
                + (nodeConfig.getNodePort(myID) + 1000));

        } catch (Exception e) {
            cleanup();
            throw new IOException("Raft startup failed", e);
        }

        startMaintenance();
    }
    
    private void setupSchema() throws IOException {
        try {
            dbSession.execute(
                "CREATE TABLE IF NOT EXISTS " + nodeIdentifier + "." + GRADE_TABLE_NAME + 
                " (id int PRIMARY KEY, events list<int>)");
            
            dbSession.execute(
                "CREATE TABLE IF NOT EXISTS " + nodeIdentifier + "." + META_TABLE_NAME + 
                " (id text PRIMARY KEY, last_applied bigint)");
            
            ResultSet rs = dbSession.execute(
                "SELECT last_applied FROM " + nodeIdentifier + "." + META_TABLE_NAME + 
                " WHERE id = 'position'");
            
            if (rs.one() == null) {
                dbSession.execute(
                    "INSERT INTO " + nodeIdentifier + "." + META_TABLE_NAME + 
                    " (id, last_applied) VALUES ('position', 0)");
            }
            
            System.out.println("[" + nodeIdentifier + "] Schema ready");
            
        } catch (Exception e) {
            throw new IOException("Schema setup failed", e);
        }
    }
    
    private List<RaftPeer> buildPeerList(NodeConfig<String> nodeConfig) {
        List<RaftPeer> peers = new ArrayList<>();
        
        for (String nodeId : nodeConfig.getNodeIDs()) {
            int raftPort = nodeConfig.getNodePort(nodeId) + 1000;
            String addr = nodeConfig.getNodeAddress(nodeId) + ":" + raftPort;
            
            RaftPeer peer = RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(nodeId))
                .setAddress(addr)
                .build();
            
            peers.add(peer);
            System.out.println("[" + nodeIdentifier + "] Peer: " + nodeId + " @ " + addr);
        }
        
        return peers;
    }
    
    private RaftProperties buildConfiguration(NodeConfig<String> nodeConfig, String myID) {
        RaftProperties props = new RaftProperties();
        
        int raftPort = nodeConfig.getNodePort(myID) + 1000;
        GrpcConfigKeys.Server.setPort(props, raftPort);
        
        Path storage = Paths.get("raft_storage", nodeIdentifier);
        RaftServerConfigKeys.setStorageDir(props, 
            Collections.singletonList(storage.toFile()));
        
        // Different timing
        RaftServerConfigKeys.Rpc.setTimeoutMin(props, 
            java.time.Duration.ofMillis(400));
        RaftServerConfigKeys.Rpc.setTimeoutMax(props, 
            java.time.Duration.ofMillis(600));
        
        RaftServerConfigKeys.Log.setSegmentSizeMax(props, 512 * 1024);
        
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(props, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(props, CHECKPOINT_THRESHOLD);
        
        return props;
    }
    
    private void startMaintenance() {
        maintenancePool.scheduleAtFixedRate(() -> {
            try {
                if (running.get()) {
                    stateMachine.takeSnapshot();
                }
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] Maintenance error: " 
                    + e.getMessage());
            }
        }, 60, 60, TimeUnit.SECONDS);
        
        maintenancePool.scheduleAtFixedRate(() -> {
            try {
                long cutoff = System.currentTimeMillis() - (OPERATION_TIMEOUT_MS * 2);
                pendingOps.entrySet().removeIf(e -> e.getValue().startTime < cutoff);
            } catch (Exception e) {
                System.err.println("[" + nodeIdentifier + "] Cleanup error: " + e.getMessage());
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String msg = new String(bytes, StandardCharsets.UTF_8).trim();
        
        if (msg.isEmpty()) {
            respond(header, "");
            return;
        }
        
        System.out.println("[" + nodeIdentifier + "] Client msg: " + msg);
        
        String opId;
        String sql;
        
        if (!msg.contains("||")) {
            opId = UUID.randomUUID().toString();
            sql = msg;
        } else {
            String[] parts = msg.split("\\|\\|", 2);
            opId = parts[0].trim();
            sql = parts.length > 1 ? parts[1].trim() : "";
        }
        
        if (sql.isEmpty()) {
            respond(header, opId + "||Empty");
            return;
        }
        
        workerPool.submit(() -> executeOperation(opId, sql, header));
    }
    
    private void executeOperation(String opId, String sql, NIOHeader header) {
        try {
            OperationHandle handle = new OperationHandle(opId);
            pendingOps.put(opId, handle);
            
            String cmd = opId + "||" + sql;
            ByteString data = ByteString.copyFromUtf8(cmd);
            
            RaftClientRequest req = RaftClientRequest.newBuilder()
                .setClientId(ClientId.valueOf(ByteString.copyFromUtf8(nodeIdentifier)))
                .setServerId(selfPeerId)
                .setGroupId(consensusGroup.getGroupId())
                .setCallId(System.nanoTime())
                .setMessage(Message.valueOf(data))
                .setType(RaftClientRequest.writeRequestType())
                .build();
            
            CompletableFuture<RaftClientReply> future = 
                raftInstance.submitClientRequestAsync(req);
            
            RaftClientReply reply = future.get(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            
            if (reply.isSuccess()) {
                String result = handle.promise.get(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                respond(header, opId + "||" + result);
            } else {
                respond(header, opId + "||FAIL");
            }
            
        } catch (TimeoutException e) {
            System.err.println("[" + nodeIdentifier + "] Timeout: " + opId);
            respond(header, opId + "||TIMEOUT");
        } catch (Exception e) {
            System.err.println("[" + nodeIdentifier + "] Error: " + opId + 
                " - " + e.getMessage());
            respond(header, opId + "||ERROR");
        } finally {
            pendingOps.remove(opId);
        }
    }
    
    private void respond(NIOHeader header, String response) {
        try {
            clientMessenger.send(header.sndr, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("[" + nodeIdentifier + "] Send failed: " + e.getMessage());
        }
    }

    @Override
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // Raft handles inter-server communication
    }
    
    private String qualifyTableName(String sql) {
        String lower = sql.toLowerCase();
        
        if (lower.contains(nodeIdentifier.toLowerCase() + ".")) {
            return sql;
        }
        
        String result = sql;
        result = result.replaceAll("(?i)\\bfrom\\s+(" + GRADE_TABLE_NAME + "|" + 
            META_TABLE_NAME + ")\\b", "from " + nodeIdentifier + ".$1");
        result = result.replaceAll("(?i)\\binto\\s+(" + GRADE_TABLE_NAME + "|" + 
            META_TABLE_NAME + ")\\b", "into " + nodeIdentifier + ".$1");
        result = result.replaceAll("(?i)\\bupdate\\s+(" + GRADE_TABLE_NAME + "|" + 
            META_TABLE_NAME + ")\\b", "update " + nodeIdentifier + ".$1");
        result = result.replaceAll("(?i)\\btruncate\\s+(" + GRADE_TABLE_NAME + "|" + 
            META_TABLE_NAME + ")\\b", "truncate " + nodeIdentifier + ".$1");
        
        return result;
    }
    
    private void cleanup() {
        running.set(false);
        
        if (workerPool != null) {
            workerPool.shutdownNow();
        }
        
        if (maintenancePool != null) {
            maintenancePool.shutdownNow();
        }
        
        if (dbSession != null && !dbSession.isClosed()) {
            dbSession.close();
        }
        
        if (dbCluster != null && !dbCluster.isClosed()) {
            dbCluster.close();
        }
    }

    @Override
    public void close() {
        System.out.println("[" + nodeIdentifier + "] Closing...");
        
        try {
            if (raftInstance != null) {
                raftInstance.close();
            }
        } catch (IOException e) {
            System.err.println("[" + nodeIdentifier + "] Raft close error: " + e.getMessage());
        }
        
        cleanup();
        
        try {
            super.close();
        } catch (Exception e) {
            System.err.println("[" + nodeIdentifier + "] Parent close error: " + e.getMessage());
        }
        
        System.out.println("[" + nodeIdentifier + "] Closed");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: MyDBFaultTolerantServerZK <config> <id>");
            System.exit(1);
        }

        NodeConfig<String> config = NodeConfigUtils.getNodeConfigFromFile(
            args[0],
            ReplicatedServer.SERVER_PREFIX,
            ReplicatedServer.SERVER_PORT_OFFSET);

        InetSocketAddress dbAddr = args.length > 2 ?
            Util.getInetSocketAddressFromString(args[2]) :
            new InetSocketAddress("localhost", 9042);

        MyDBFaultTolerantServerZK server = new MyDBFaultTolerantServerZK(
            config, args[1], dbAddr);
        
        System.out.println("Server " + args[1] + " running");
        
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    }
}
