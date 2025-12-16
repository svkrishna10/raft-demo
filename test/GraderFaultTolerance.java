import com.datastax.driver.core.ResultSet;
import com.gradescope.jh61b.grader.GradedTest;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runners.MethodSorters;
import server.ReplicatedServer;
import server.faulttolerance.MyDBFaultTolerantServerZK;
import server.faulttolerance.MyDBReplicableAppGP;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The tests in this class test fault tolerance.
 * <p>
 * You can also replace "extends GraderCommonSetup" below with "extends
 * GraderConsistency" if you wish to run tests for both simple (non-fault-prone)
 * replication and fault-tolerance sequentially in one fell swoop.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GraderFaultTolerance extends GraderCommonSetup {

/**
 * True if Gigapaxos being used, false if Zookeeper or anything else.
 */
public static final boolean GIGAPAXOS_MODE =
		Boolean.getBoolean("gigapaxos.mode");//false;

/**
 * Maximum permitted size of any collection that is used to maintain
 * request-specific state, i.e., you can not maintain state for more than
 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
 * ensure that your logs don't grow unbounded, which forces
 * checkpointing to be implemented.
 */
public static final int MAX_LOG_SIZE = 400;


// If true, replicas will start with whatever DB table state they had
// just before they last crashed. Should be false while submitting.
// But you can set it to true for debugging/testing tests that don't
// involve checkpointing.
protected static boolean DISABLE_RECOVERING_WITH_EMPTY_STATE = false;

@BeforeClass
public static void setupFT() throws IOException, InterruptedException {
		/*
	Best to remove paxos_logs with each test to avoid errors from carrying
	over. Put here defensively in case students forget to do so manually.
	 */
	//if(GIGAPAXOS_MODE)
		Util.recursiveRemove(new File("paxos_logs"));
	// kill all started processes before shutdown
	addShutdownHook();

	GraderCommonSetup.setup(GIGAPAXOS_MODE);
	if (!GraderFaultTolerance.DISABLE_RECOVERING_WITH_EMPTY_STATE)
		createEmptyTables();

	if (!PROCESS_MODE) {
		// gigapaxos mode single-JVM setup is a bit different from
		// MyDBFaultTolerantServerZK or the
		// simple non-fault-tolerant MyDBReplicatedServer and also needs a
		// longer bootstrap time
		if (GIGAPAXOS_MODE) {
			// start gigapaxos servers
			setupGP();

		}
		// instantiate MyDBFaultTolerantServerZK objects
		else startReplicatedServersSingleJVM(true);
	}
	else ServerFailureRecoveryManager.startAllServers(true);

	System.out.println("\nWaiting (" + (PER_SERVER_BOOTSTRAP_TIME * (servers.length)) / 1000 + " seconds) for " + (GIGAPAXOS_MODE? "gigapaxos":"ZK-or-other-approach") + " servers to start");
	// sleep to allow servers to bootup
	Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * (servers.length));
}

private static Set<ReconfigurableNode> gpServers =
		new HashSet<ReconfigurableNode>();

private static void setupGP() throws IOException, InterruptedException {
	System.setProperty("gigapaxosConfig", "conf/gigapaxos.properties");
	System.setProperty("java.util.logging.config.file", "logging" + "" + "" +
			".properties");
	System.out.println("Application = " + Config.getGlobalString(PaxosConfig.PC.APPLICATION));
	//if (DEFER_SERVER_CREATION_TO_CHILD)
	{
		// Need to create empty tables before gigapaxos servers wake up as
		// they are expected to issue DB requests while rolling forward
		// from the most recent checkpoint during recovery
		createEmptyTables();
		int i = 0;
		for (String node : servers) {
			Set<ReconfigurableNode> created =
					(ReconfigurableNode.main1(Arrays.asList(node, "start",
							node).toArray(new String[0])));
			for (ReconfigurableNode createdElement : created)
				gpServers.add(createdElement);
			System.out.println("Started node " + node);
		}
	}
}


private static String getCommand(String cmd) {
	return !GIGAPAXOS_MODE ? cmd :
			new RequestPacket(cmd, false).putPaxosID((STUDENT_TESTING_MODE ?
					MyDBReplicableAppGP.class.getSimpleName() :
					"AVDBReplicableAppGP") + "0", 0).toString();
}

private static InetSocketAddress getAddress(String server) {
	return !GIGAPAXOS_MODE ? serverMap.get(server) :
			new InetSocketAddress(serverMap.get(server).getAddress(),
					ReconfigurationConfig.getClientFacingPort(serverMap.get(server).getPort()));
}

/**
 * Issue a request to one server and check that it executed successfully on
 * all servers in graceful (failure-free) scenarios. This test should pass
 * even with the simple non-fault-tolerant replicated server and is simply
 * here for making sure that your fault-tolerant server works as
 * expected at
 * least in failure-free scenarios.
 * <p>
 * Note that the {@link GraderFaultTolerance#getCommand(String)} method
 * will
 * always send requests by wrapping them up into {@link RequestPacket}
 * packet type for gigapaxos mode, so no special packet type definitions
 * are
 * needed.
 */
@Test
@GradedTest(name = "test31_GracefulExecutionSingleRequest()", max_score = 1)
public void test31_GracefulExecutionSingleRequest() throws IOException,
		InterruptedException {

	int key = ThreadLocalRandom.current().nextInt();

	String server = servers[0];

	// single update after insert
	client.send(serverMap.get(server), getCommand(insertRecordIntoTableCmd(key
			, DEFAULT_TABLE_NAME)));
	Thread.sleep(SLEEP);
	client.send(serverMap.get(server), getCommand(updateRecordOfTableCmd(key,
			DEFAULT_TABLE_NAME)));
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}

/**
 * Graceful execution as above but with multiple requests to single server.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
@GradedTest(name = "test32_GracefulExecutionMultipleRequestsSingleServer()",
		max_score = 1)
public void test32_GracefulExecutionMultipleRequestsSingleServer() throws IOException, InterruptedException {

	int key = ThreadLocalRandom.current().nextInt();

	// random server
	String server = servers[Math.abs(key % servers.length)];
	client.send(serverMap.get(server), getCommand(insertRecordIntoTableCmd(key
			, DEFAULT_TABLE_NAME)));
	Thread.sleep(SLEEP);

	// number of requests is arbitrary
	for (int i = 0; i < servers.length * 2; i++) {
		client.send(serverMap.get(server),
				getCommand(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
	}
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}

@Test
@GradedTest(name = "test33_GracefulExecutionMultipleRequestsToMultipleServers" +
		"()", max_score = 2)
public void test33_GracefulExecutionMultipleRequestsToMultipleServers() throws IOException, InterruptedException {

	int key = ThreadLocalRandom.current().nextInt();

	// random server
	String server = servers[Math.abs(key % servers.length)];
	client.send(serverMap.get(server), getCommand(insertRecordIntoTableCmd(key
			, DEFAULT_TABLE_NAME)));
	Thread.sleep(SLEEP);

	// number of requests is arbitrary
	for (int i = 0; i < servers.length * 2; i++) {
		client.send(getRandomServerAddr(),
				getCommand(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
	}
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}


private static Set<String> crashed = new HashSet<String>();

/**
 * Crash one random server, issue one request, check successful execution
 * across alive servers.
 */
@Test
@GradedTest(name = "test34_SingleServerCrash()", max_score = 4)
public void test34_SingleServerCrash() throws IOException,
		InterruptedException {

	int key = ThreadLocalRandom.current().nextInt();

	String victim = ServerFailureRecoveryManager.killRandomServer();
	Assert.assertTrue("Failed to crash any server because fault " + "tolerance" +
			" tests are being run with PROCESS_MODE=" + PROCESS_MODE + "; " +
			"exiting erroneously", victim != null);
	crashed.add(victim);
	String server = (String) Util.getRandomOtherThan(serverMap.keySet(),
			victim);

	System.out.println("Sending command to " + server + "; crashed=" + crashed);

	// We send requests slowly in a loop until the first one succeeds so
	// that we can be sure that the key record got created and subsequent
	// value insertions will succeed. Upon a crash, if the crashed server
	// was the coordinator, some requests may intermittently fail until a
	// another node realizes the state of affairs. and decides to take over
	// as coordinator.
	int count=0;
	do {
		client.send(serverMap.get(server),
				getCommand(insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME)));
		Thread.sleep(SLEEP);
	} while (serverMap.size() > crashed.size() * 2 //majority
			&& verifyInserted(key, server) && ++count<15);

	Assert.assertTrue("Unable to create record with a single crashed server",
			count<15);
	for (int i = 0; i < servers.length * 2; i++) {
		client.send(serverMap.get((String) Util.getRandomOtherThan(serverMap.keySet(), crashed)), getCommand(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
	}
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
			// nonempty only if majority availability
			serverMap.size() <= 2 * crashed.size());
}


/**
 * Keep the same random server crashed as in the previous test, issue
 * multiple requests to different remaining servers, check successful
 * execution across alive servers.
 */
@Test
@GradedTest(name = "test35_TwoServerCrash()", max_score = 3)
public void test35_TwoServerCrash() throws IOException, InterruptedException {

	int key = ThreadLocalRandom.current().nextInt();

	String victim1 = crashed.iterator().next();
	String victim2 = (String) Util.getRandomOtherThan(serverMap.keySet(),
			victim1);
	ServerFailureRecoveryManager.killServer(victim2);
	crashed.add(victim2);


	String server = (String) Util.getRandomOtherThan(serverMap.keySet(),
			crashed);
	Assert.assertTrue(!server.equals(crashed.iterator().next()));
	System.out.println("Sending request to " + server + "; " + "crashed=" + crashed);

	// while loop below won't actually be repeated because of lack of majority
	do {
		client.send(serverMap.get(server),
				getCommand(insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME)));
		Thread.sleep(SLEEP);
	} while (serverMap.size() > crashed.size() * 2 //majority
			&& verifyInserted(key, server));

	for (int i = 0; i < servers.length * 3; i++) {
		client.send(serverMap.get((String) Util.getRandomOtherThan(serverMap.keySet(), crashed)), getCommand(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
	}
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
			// can be empty (no liveness) if less than majority up
			serverMap.size() <= 2 * crashed.size());
}

/**
 * Recovers one of the crashed servers.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
@GradedTest(name = "test36_OneServerRecoveryMultipleRequests()", max_score = 3)
public void test36_OneServerRecoveryMultipleRequests() throws IOException,
		InterruptedException {
	String first = crashed.iterator().next();
	ServerFailureRecoveryManager.recoverServer(first);
	Thread.sleep(PER_SERVER_BOOTSTRAP_TIME*2);
	crashed.remove(first);

	int key = ThreadLocalRandom.current().nextInt();

	int count=0;
	String server;
	// No need to wait or sleep here as a recovering server shouldn't be
	// a reason for a request to be lost.
	do {
		client.send(serverMap.get(server = (String) Util.getRandomOtherThan(serverMap.keySet(), crashed)), getCommand(insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME)));
		Thread.sleep(SLEEP);
	} while (serverMap.size() > crashed.size() * 2 //majority
			&& verifyInserted(key, server) && ++count<15);

	Assert.assertTrue("key " + key + " not inserted at entry server " + server, !verifyInserted(key, server));

	String cmd = null;
	for (int i = 0; i < servers.length * 3; i++) {
		client.send(serverMap.get((String) Util.getRandomOtherThan(serverMap.keySet(), crashed)), cmd = getCommand(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
	}
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
			// can be empty (no liveness) if less than majority up
			serverMap.size() <= 2 * crashed.size());
}

private static Integer fixedKeyKnownToExist = null;

/**
 * Recovers the second crashed server. Equivalent to running the previous
 * test a second time.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
@GradedTest(name = "test37_TwoServerRecoveryMultipleRequests()", max_score = 3)
public void test37_TwoServerRecoveryMultipleRequests() throws IOException,
		InterruptedException {
	String first = crashed.iterator().next();
	ServerFailureRecoveryManager.recoverServer(first);
	Thread.sleep(PER_SERVER_BOOTSTRAP_TIME);
	crashed.remove(first);

	int key = (fixedKeyKnownToExist = ThreadLocalRandom.current().nextInt());
	String server;
	int count=0;
	do {
	client.send(serverMap.get(server =
			(String) Util.getRandomOtherThan(serverMap.keySet(), crashed)), getCommand(insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME)));
	Thread.sleep(SLEEP);
	} while (serverMap.size() > crashed.size() * 2 //majority
			&& verifyInserted(key, server) && ++count<15);

	for (int i = 0; i < servers.length * 3; i++) {
		client.send(serverMap.get((String) Util.getRandomOtherThan(serverMap.keySet(), crashed)), getCommand(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
	}
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
			// can be empty (no liveness) if less than majority up
			serverMap.size() <= 2 * crashed.size());
}

/**
 * Checks that the entire table matches across all servers. This test
 * requires that recovering replicas can recover to a recent checkpoint of
 * the state and replay missed requests from there onwards to catch up with
 * what transpired when they were crashed.
 * <p>
 * There is always the default checkpoint of the empty initial state, but
 * not checkpointing periodically means that the log of requests to replay
 * while rolling forward upon recovery would keep growing unbounded.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
@GradedTest(name = "test38_EntireStateMatchCheck()", max_score = 3)
public void test38_EntireStateMatchCheck() throws IOException,
		InterruptedException {
	verifyTableConsistent(DEFAULT_TABLE_NAME);
}

/**
 * This test kills all servers simultaneously and then recovers them
 * simultaneously. No requests are issued. Entire state should match at the
 * end.
 *
 * @throws IOException
 * @throws InterruptedException
 */

@Test
@GradedTest(name = "test39_InstantaneousMassacreAndRevivalTest()", max_score
		= 3)
public void test39_InstantaneousMassacreAndRevivalTest() throws IOException,
		InterruptedException {
	ServerFailureRecoveryManager.killAllServers();
	ServerFailureRecoveryManager.startAllServers();
	Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * servers.length);
	test38_EntireStateMatchCheck();
}

/**
 * Kills all servers serially and then recovers them serially, all while
 * spraying requests at the servers.
 * <p>
 * Because request spraying is happening concurrently with violent massacre
 * and benevolent rebirths, not all requests will be executed (liveness),
 * but some should get executed, and the state should be identical at the
 * end (safety). Furthermore, the key record is guaranteed to satisfy the
 * non-emptiness check in
 * {@link GraderFaultTolerance#verifyOrderConsistent}
 * because of an earlier test that already populated the record with
 * nonempty values.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
@GradedTest(name = "test40_SerialKillAndRecover()", max_score = 3)
public void test40_SerialKillAndRecover() throws IOException,
		InterruptedException {
	long interKillIntervalMillis = 500, interRecoverMillis = 800;
	// This method is asynchronous and will schedule and return
	// immediately.
	ServerFailureRecoveryManager.serialKillAndThenSerialRecover(servers.length
			, interKillIntervalMillis, interRecoverMillis);
	// no bootstrap time needed here

	// Spray requests while slaughter and rebirth is happening
	for (int i = 0; i < servers.length*servers.length; i++) {
		client.send(serverMap.get(getRandomServerAddr()),
				getCommand(updateRecordOfTableCmd(fixedKeyKnownToExist,
						DEFAULT_TABLE_NAME)));
		Thread.sleep(Math.min(interKillIntervalMillis, interRecoverMillis) / 3);
	}
	// sleep long enough for all servers to have recovered
	Thread.sleep((long) (PER_SERVER_BOOTSTRAP_TIME * servers.length + (interKillIntervalMillis + interRecoverMillis) * (1 + ServerFailureRecoveryManager.PERTURB_FRAC)));

	verifyOrderConsistent(DEFAULT_TABLE_NAME, fixedKeyKnownToExist);
}


/**
 * This test tests checkpointing of state and restoration upon recovery
 * by issuing more than MAX_LOG_SIZE requests.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
@GradedTest(name = "test41_CheckpointRecoveryTest()", max_score = 30)
public void test41_CheckpointRecoveryTest() throws IOException,
		InterruptedException {

	// issue enough requests to stress test checkpointing
	for (int i = 0; i < MAX_LOG_SIZE * 5 + servers.length; i++) {
		client.send(serverMap.get(servers[0]),
				getCommand(updateRecordOfTableCmd(fixedKeyKnownToExist,
						DEFAULT_TABLE_NAME)));
		// small sleep to slow down because gigapaxos is optimized to
		// batch request flurries and may effectively see fewer
		// requests.
		Thread.sleep(10);
	}
	Thread.sleep(SLEEP*4);
	int numCommittedEventsForFixedKeyBefore =
			getMinNumCommittedEventsForFixedKey(fixedKeyKnownToExist);

	ServerFailureRecoveryManager.mercilesslySlaughterAll();
	ServerFailureRecoveryManager.startAllServers();
	Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * servers.length*2);

	// specific key
	verifyOrderConsistent(DEFAULT_TABLE_NAME, fixedKeyKnownToExist);
	// entire state
	test38_EntireStateMatchCheck();
	int numCommittedEventsForFixedKeyAfter =
			getMinNumCommittedEventsForFixedKey(fixedKeyKnownToExist);
	Assert.assertTrue("Number of committed requests post-crash is lower " +
			"compared to pre-crash, probably because checkpoint and restore" +
					" " +
			"has not been implemented correctly",
			numCommittedEventsForFixedKeyAfter >= numCommittedEventsForFixedKeyBefore);
}


/**
 * Clean up the default tables. Will always succeed irrespective of student
 * code.
 * <p>
 * For debugging purposes, if you wish to manually inspect the tables to
 * see
 * what's not matching, you can temporarily disable this test in your local
 * copy. {@link GraderFaultTolerance} tests will always clear the table
 * before the first test, so you don't have to worry about cleaning
 * leftover state from previous runs.
 *
 * @throws InterruptedException
 */
@Test
public void test49_DropTables() throws InterruptedException {
	Thread.sleep(MAX_SLEEP);
	if(GIGAPAXOS_MODE|| MyDBFaultTolerantServerZK.DROP_TABLES_AFTER_TESTS)
	for (String node : servers) {
		// clean up
		session.execute(getDropTableCmd(DEFAULT_TABLE_NAME, node));
	}
}

/**
 * This test should always pass. Unless unnecessary timers or threads are
 * introduced by students.
 */
@Test
public void test99_closeSessionAndServers() {

	session.close();
	cluster.close();
	// If gigapaxos and single JVM servers, we need to close created
	// classes. If separate processes, the shutdown hook automatically
	// kills them.
	if (GIGAPAXOS_MODE /*&& DEFER_SERVER_CREATION_TO_CHILD*/)
		for (ReconfigurableNode node : gpServers)
			node.close();
}

private static InetSocketAddress getRandomServerAddr() {
	return serverMap.get(ServerFailureRecoveryManager.getRandomServer());
}

// verify that the key actually exists on server
private boolean verifyInserted(int key, String server) {
	ResultSet result = session.execute(readResultFromTableCmd(key,
			DEFAULT_TABLE_NAME, server));
	return result.isExhausted();
}

protected void verifyTableConsistent(String table) {
	verifyTableConsistent(table, false);
}

protected void verifyTableConsistent(String table, boolean possiblyEmpty) {
	verifyOrderConsistent(table, (Integer) null, new HashSet<String>(),
			possiblyEmpty);
}


protected void verifyOrderConsistent(String table, int key,
									 Set<String> exclude) {
	verifyOrderConsistent(table, key, exclude, false);
}

static {
	System.setProperty("gigapaxosConfig", "conf/gigapaxos.properties");
}

public static void main(String[] args) throws IOException {
		JUnitCore.runClasses(GraderFaultTolerance.class);
}
}
