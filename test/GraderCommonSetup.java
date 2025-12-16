import client.Client;
import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.RepeatRule;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import server.*;
import server.faulttolerance.MyDBFaultTolerantServerZK;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class sets up the testing environment needed by GraderConsistency.
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderCommonSetup {

/**
 * For SingleServer, {@link #DEFAULT_KEYSPACE} is the default keyspace. For
 * replicated
 * servers, each server uses a keyspace named after the server's name itself.
 * All needed keyspaces are being created in the setup() method below.
 **/
protected static final String DEFAULT_KEYSPACE = "demo";
protected static final InetSocketAddress DEFAULT_SADDR =
		new InetSocketAddress("localhost", 1999);
private static final InetSocketAddress DEFAULT_DB_ADDR =
		new InetSocketAddress("localhost", 9042);
// This is the table storing all server-side state. By assumption, the
// state in this table in the keyspace corresponding to each replica is
// all of the safety-critical state to be managed consistently.
protected final static String DEFAULT_TABLE_NAME = "grade";
protected static Cluster cluster;
protected static Session session = null;
////////////end of DB params

////////////// client/server config variables
protected static Client client = null;
//private static SingleServer singleServer = null;
private static SingleServer[] replicatedServers = null;
protected static Map<String, InetSocketAddress> serverMap = null;
protected static String[] servers = null;
protected static NodeConfig<String> nodeConfigServer;
/*
 Protected because ServerFailureRecoveryManager also accesses this
 information to provide it as a command-line arg to start servers as
 separate processes.
 */
protected static String CONFIG_FILE;
////////////////// end of client/server config variables

// a commonly used number of requests to test with
protected static final int NUM_REQS = 100;

//////////////////// sleep macros
protected static final int MAX_SLEEP = 1000;
protected static final int SLEEP_RATIO = 10;
// ZK or other implementations can set higher sleep if needed, but it
// shouldn't be needed.
protected static int SLEEP = Math.max(MAX_SLEEP,
		MyDBFaultTolerantServerZK.SLEEP);
public static final int PER_SERVER_BOOTSTRAP_TIME = MAX_SLEEP * 6;
/////////////////////// end of SLEEP params

/* True means servers will be started as separate OS-level processes
as opposed to objects all instantiated within a single JVM.
 */
protected static final boolean PROCESS_MODE = true;


/* Set to true if you want each server's output and error in a separate
file named <node>.out where <node> is the server's name from the
conf.properties file.
 */
protected static final boolean REDIRECT_IO_TO_FILE = false;


/* Must be true when used by students.
 */
protected static final boolean STUDENT_TESTING_MODE = true;


//@BeforeClass
public static void setup(boolean gpMode) throws IOException,
		InterruptedException {

	session = (cluster =
			Cluster.builder().addContactPoint(DEFAULT_SADDR.getHostName()).build()).connect(DEFAULT_KEYSPACE);

	CONFIG_FILE = System.getProperty("config") != null ? System.getProperty(
			"config") : (!gpMode ? "conf/servers.properties" : "conf/gigapaxos"
			+ ".properties");

	nodeConfigServer = NodeConfigUtils.getNodeConfigFromFile(CONFIG_FILE,
			(!gpMode ? ReplicatedServer.SERVER_PREFIX :
					PaxosConfig.DEFAULT_SERVER_PREFIX), (!gpMode ?
					ReplicatedServer.SERVER_PORT_OFFSET : 0));

		/* Setup client here. Will typically instantiate MyDBClient here
		because
		STUDENT_MODE is true by default.
		 */
	NodeConfig<String> nodeConfigClient =
			NodeConfigUtils.getNodeConfigFromFile(CONFIG_FILE, (!gpMode ?
					ReplicatedServer.SERVER_PREFIX :
					PaxosConfig.DEFAULT_SERVER_PREFIX), (!gpMode ? 0 :
					PaxosConfig.getClientPortOffset()));
	client = STUDENT_TESTING_MODE ? new MyDBClient(nodeConfigClient)

			// instructor client
			: (Client) getInstance(getConstructor("client.AVDBClient",
			NodeConfig.class), nodeConfigClient);


	// setup replicated servers and sessions to test
	replicatedServers = new SingleServer[nodeConfigServer.getNodeIDs().size()];

	// create keyspaces if not exists
	session.execute("create keyspace if not exists " + DEFAULT_KEYSPACE + " " + "with " +
			"replication={'class':'SimpleStrategy', 'replication_factor' : '1'};");
	// create per-server keyspace and table if not exists
	for (String node : nodeConfigServer.getNodeIDs()) {
		session.execute("create keyspace if not exists " + node + " with " +
				"replication={'class':'SimpleStrategy', " +
				"'replication_factor' : '1'};");
		session.execute(getCreateTableWithList(DEFAULT_TABLE_NAME, node));

	}

	// setup frequently used information
	int i = 0;
	servers = new String[nodeConfigServer.getNodeIDs().size()];
	for (String node : nodeConfigServer.getNodeIDs())
		servers[i++] = node;
	serverMap = new HashMap<String, InetSocketAddress>();
	for (String node : nodeConfigClient.getNodeIDs())
		serverMap.put(node,
				new InetSocketAddress(nodeConfigClient.getNodeAddress(node),
						nodeConfigClient.getNodePort(node)));

	// Servers may be set up either as instantiated objects all within a
	// single JVM or as separate processes.
//	if(PROCESS_MODE) ServerFailureRecoveryManager.startAllServers();
//	else /*do nothing here*/; // single JVM server creation in child classes
	//startReplicatedServers();
}


//////////////////////// Test watching setup ///////////////////
@Rule
public TestName testName = new TestName();
@Rule
public RepeatRule repeatRule = new RepeatRule();
@Rule
public TestWatcher watcher = new TestWatcher() {
	protected void failed(Throwable e, Description description) {
		System.out.println(" FAILED!!!!!!!!!!!!! " + e);
		e.printStackTrace();
		System.exit(1);
	}

	protected void succeeded(Description description) {
		System.out.println(" succeeded");
	}
};

@Before
public void beforeMethod() {
	System.out.print(this.testName.getMethodName() + " ");
}
/////////////////////////////////////////////////////////

protected static void createEmptyTables() throws InterruptedException {
	for (String node : servers) {
		// create default table (if not exists)  with node as the keypsace name
		session.execute(getCreateTableWithList(DEFAULT_TABLE_NAME, node));
		session.execute(getClearTableCmd(DEFAULT_TABLE_NAME, node));
	}
}

protected static void clearTable(String node) throws InterruptedException {
	// create default table (if not exists)  with node as the keypsace name
	session.execute(getClearTableCmd(DEFAULT_TABLE_NAME, node));
}


protected void testCreateTable(boolean single, boolean sleep) throws IOException, InterruptedException {
	if (sleep) testCreateTableSleep(single);
	else testCreateTableBlocking();
	verifyTableExists(TABLE, DEFAULT_KEYSPACE, true);

}

protected void verifyTableExists(String table, String keyspace,
								 boolean exists) {
	ResultSet resultSet = session.execute("select table_name from " +
			"system_schema.tables where keyspace_name='" + keyspace + "'");
	Assert.assertTrue(!resultSet.isExhausted());
	boolean match = false;
	for (Row row : resultSet)
		match = match || row.getString("table_name").equals(table);
	if (exists) Assert.assertTrue(match);
	else Assert.assertFalse(match);
}

private boolean ALL_VALUES_NONEMPTY = false;

protected void verifyOrderConsistent(String table, int key) {
	verifyOrderConsistent(table, key, new HashSet<String>(), false);
}

protected void verifyOrderConsistent(String table, Integer key,
									 Set<String> exclude,
									 boolean possiblyEmpty) {
	verifyOrderConsistent(table, key, exclude, possiblyEmpty, true);
									 }

protected void verifyOrderConsistent(String table, Integer key,
									 Set<String> exclude,
									 boolean possiblyEmpty,
									 boolean prefixOkay) {
	String[] results = new String[servers.length - exclude.size()];
	String[] comparedServers = new String[servers.length - exclude.size()];

	int j = 0;
	for (int i = 0; i < servers.length; i++)
		if (!exclude.contains(servers[i])) comparedServers[j++] = servers[i];

	int i = 0;
	boolean nonEmpty = false, nonEmptyValues = true;
	ResultSet[] resultSets = new ResultSet[comparedServers.length];
	Map<Integer,ArrayList<Integer>>[] tables = new Map[comparedServers.length];

	for (String node : comparedServers) {
		resultSets[i] = session.execute(key != null ?
				readResultFromTableCmd(key, table, node) :
				readResultFromTableCmd(table, node));

		tables[i] = new HashMap<Integer, ArrayList<Integer>>();
		for(Row row : resultSets[i]) {
			//nonEmpty = true;
			// single key row mode with each row an events list
			if(key!=null) {
				tables[i].putIfAbsent(key, new ArrayList<Integer>());
				tables[i].put(key, new ArrayList<Integer>(row.getList("events",
						Integer.class)));
				nonEmpty = nonEmptyValues = !tables[i].get(key).isEmpty();
			}
			// entire table mode with each row a key:events_list pair
			else {
				int curKey = row.getInt(0);
				tables[i].put(curKey,new ArrayList<Integer>(row.getList(1,
						Integer.class)));
				// at least one key has nonemptyvalues
				nonEmpty = nonEmpty || (!tables[i].get(curKey).isEmpty());
				// all keys have nonempty values
				nonEmptyValues =
						nonEmptyValues && (!tables[i].get(curKey).isEmpty());
			}
		}
		i++;
	}
	if(ALL_VALUES_NONEMPTY) nonEmpty = nonEmpty && nonEmptyValues;

	Map<Integer,ArrayList<Integer>> longest = tables[0];
	int longestIndex = 0; boolean done = false;
	for (i=0; i<tables.length;i++) {
		if (tables[i].size() > longest.size()) {
			longest = tables[i];
			longestIndex = i;
		}
	}

	boolean match = true;
	String message = "";
	for (i = 0; i < tables.length; i++) {
		if(longest == tables[i]) continue;
		// exact match or prefix check
		if ((!prefixOkay && !longest.equals(tables[i]))
				||
				// prefix check
				(prefixOkay && !isPrefix(longest,tables[i]))
		) {
			match = false;
			message += "\n" + comparedServers[longestIndex] + ":" + tables[longestIndex] + "\n " +
					"!="+(prefixOkay ? "(prefix)":"(exact)")+"\n" + comparedServers[i] +
					":" + tables[i] + "\n";
		}
	}


	message += (!(possiblyEmpty || nonEmpty) ? "nonEmpty=" + nonEmpty : "");
	if (!exclude.isEmpty()) System.out.println("Excluded servers=" + exclude);
	System.out.println("\n");
	for (i = 0; i < results.length; i++)
		//System.out.println(comparedServers[i] + ":" + results[i]);
		System.out.println(comparedServers[i] + ":" + tables[i]);
		Assert.assertTrue(message, (possiblyEmpty || nonEmpty) && match);
}

private boolean isPrefix(Map<Integer,ArrayList<Integer>> longerMap, Map<Integer,
		ArrayList<Integer>> map) {
	boolean unequal = longerMap.size()!=map.size();
	int nonExactMatches = 0;
	boolean prefixMismatch = false;
	for(Integer key : map.keySet()) {
		if(!map.get(key).equals(longerMap.get(key))) nonExactMatches++;
		if(!isPrefix(map.get(key),longerMap.get(key))) prefixMismatch = true;
	}
	return (nonExactMatches<=1) && !prefixMismatch;
}


private boolean isPrefix(ArrayList<Integer> a1, ArrayList<Integer> a2) {
	for (int i = 0; i < Math.min(a1.size(), a2.size()); i++)
		if (!a1.get(i).equals(a2.get(i))) return false;
	return true;
}

private void testCreateTableSleep(boolean single) throws InterruptedException
		, IOException {
	send(getDropTableCmd(TABLE, DEFAULT_KEYSPACE), single);
	Thread.sleep(SLEEP);
	send(getCreateTableCmd(TABLE, DEFAULT_KEYSPACE), single);
	Thread.sleep(SLEEP);
}


ConcurrentHashMap<Long, String> outstanding = new ConcurrentHashMap<Long,
		String>();

private void testCreateTableBlocking() throws InterruptedException,
		IOException {
	waitResponse(callbackSend(DEFAULT_SADDR, getDropTableCmd(TABLE,
			DEFAULT_KEYSPACE)));
	waitResponse(callbackSend(DEFAULT_SADDR, getCreateTableCmd(TABLE,
			DEFAULT_KEYSPACE)));
}


protected Long callbackSend(InetSocketAddress isa, String request) throws IOException {
	Long id = enqueueRequest(request);
	client.callbackSend(isa, request, new WaitCallback(id));
	return id;
}

protected Long callbackSend(InetSocketAddress isa, String request,
							long timeout) throws IOException {
	Long id = enqueueRequest(request);
	client.callbackSend(isa, request, new WaitCallback(id));
	return id;
}

private class WaitCallback implements Client.Callback {
	Long monitor; // both id and monitor

	WaitCallback(Long monitor) {
		this.monitor = monitor;
	}

	@Override
	public void handleResponse(byte[] bytes, NIOHeader header) {
		synchronized (this.monitor) {
			outstanding.remove(monitor);
			this.monitor.notify();
		}
	}
}

private long reqnum = 0;

private long enqueue() {
	synchronized (outstanding) {
		return reqnum++;
	}
}

private long enqueueRequest(String request) {
	long id = enqueue();
	outstanding.put(id, request);
	return id;
}

protected void waitResponse(Long id) {
	synchronized (id) {
		while (outstanding.containsKey(id)) try {
			id.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

protected void waitResponse(Long id, long timeout) {
	synchronized (id) {
		while (outstanding.containsKey(id)) try {
			id.wait(timeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}


protected static final void send(String cmd, boolean single) throws IOException {
	client.send(single ? DEFAULT_SADDR :
			serverMap.get(servers[(int) (Math.random() * servers.length)]),
			cmd);
}

protected static final String TABLE = "users";

private static String getCreateTableCmd(String table, String keyspace) {
	return "create table if not exists " + keyspace + "." + table + " " +
			"(age" + " int, firstname " + "text, lastname text, ssn int, " +
			"address " + "text, hash bigint, " + "primary key (ssn))";
}

protected static String getDropTableCmd(String table, String keyspace) {
	return "drop table if exists " + keyspace + "." + table;
}

protected static String getClearTableCmd(String table, String keyspace) {
	return "truncate " + keyspace + "." + table;
}

protected static String getCreateTableWithList(String table, String keyspace) {
	return "create table if not exists " + keyspace + "." + table + " (id " +
			"" + "" + "" + "" + "" + "" + "" + "" + "" + "int," + " " +
			"events" + "" + "" + " " + "list<int>, " + "primary " + "" + "key" + " " + "" + "" + "" + "(id)" + ");";
}

protected static String insertRecordIntoTableCmd(int key, String table) {
	return "insert into " + table + " (id, events) values (" + key + ", " +
			"[]);";
}

protected static String updateRecordOfTableCmd(int key, String table) {
	return "update " + table + " SET events=events+[" + incrSeq() + "] " +
			"where id=" + key + ";";
}

// reads the entire table, all keys
protected static String readResultFromTableCmd(String table, String keyspace) {
	return "select * from " + keyspace + "." + table + ";";
}

// This is only used to fetch the result from the table by session
// directly connected to cassandra
protected static String readResultFromTableCmd(int key, String table,
											   String keyspace) {
	return "select events from " + keyspace + "." + table + " where id=" + key + ";";
}

// compute minimum number of committed events
protected int getMinNumCommittedEventsForFixedKey(Integer fixedKeyKnownToExist) {
	int numEvents = Integer.MAX_VALUE;
	for (String node : servers) {
		ResultSet resultSet =
				session.execute(readResultFromTableCmd(fixedKeyKnownToExist,
						DEFAULT_TABLE_NAME, node));
		for (Row row : resultSet) {
			ArrayList<Integer> list = new ArrayList<Integer>(row.getList(
					"events", Integer.class));
			if (list.size() < numEvents) numEvents = list.size();
		}
	}
	return numEvents;
}

private static long sequencer = 0;

synchronized static long incrSeq() {
	return sequencer++;
}

protected static void startReplicatedServersSingleJVM(boolean FT) throws IOException {
	int i = 0;
	for (String node : nodeConfigServer.getNodeIDs()) {
		replicatedServers[i++] = STUDENT_TESTING_MODE ?

				(FT ? new MyDBFaultTolerantServerZK(nodeConfigServer, node,
						DEFAULT_DB_ADDR) :
						new MyDBReplicatedServer(nodeConfigServer, node,
								DEFAULT_DB_ADDR))

				:

				// instructor mode
				(SingleServer) getInstance(getConstructor("server" +
						".AVDBReplicatedServer", NodeConfig.class,
						String.class, InetSocketAddress.class),
						nodeConfigServer, node, DEFAULT_DB_ADDR);
	}
}

protected static boolean LOOP_MODE=false;

@AfterClass
public static void teardown() {
	if (client != null) client.close();
//	if (singleServer != null) singleServer.close();
	session.close();
	cluster.close();

	if (PROCESS_MODE) ServerFailureRecoveryManager.killAllServers();
	else if (replicatedServers != null) for (SingleServer s :
			replicatedServers)
		if (s != null) s.close();

		if(!LOOP_MODE)
	ServerFailureRecoveryManager.scheduler.shutdownNow();

}

protected static Object getInstance(Constructor<?> constructor,
									Object... args) {
	try {
		return constructor.newInstance(args);
	} catch (InstantiationException e) {
		e.printStackTrace();
	} catch (IllegalAccessException e) {
		e.printStackTrace();
	} catch (InvocationTargetException e) {
		e.printStackTrace();
	}
	return null;
}

protected static Constructor<?> getConstructor(String clazz,
											   Class<?>... types) {
	try {
		Class<?> instance = Class.forName(clazz);
		return instance.getConstructor(types);
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
	} catch (NoSuchMethodException e) {
		e.printStackTrace();
	}
	return null;
}

protected static void addShutdownHook() {
	Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() {
			if (PROCESS_MODE) ServerFailureRecoveryManager.killAllServers();
			;
		}
	});
}

public static void main(String[] args) throws IOException {
	//addShutdownHook();
	//Result result = JUnitCore.runClasses(GraderCommonSetup.class);
}
}
