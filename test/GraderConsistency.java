import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import com.datastax.driver.core.ResultSet;

/**
 * The testing setup for tests in this class are in the parent class. This class
 * is the same as the one used in the consistency-only (no fault-tolerance)
 * assignment.
 *
 * @@see GraderCommonSetup. The tests herein test MyDBSingleServer and
 * MyDBReplicatedServer implementations.
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderConsistency extends GraderCommonSetup {

@BeforeClass
public static void setup() throws IOException, InterruptedException {
	GraderCommonSetup.setup(false);
	startReplicatedServers();
}

private static void startReplicatedServers() throws IOException,
		InterruptedException {

	if (PROCESS_MODE) {
		ServerFailureRecoveryManager.startAllServers(false);
		// Processes take some time to get initialized. Without the sleep,
		// the static initialization code doesn't finish executing before the
		// class has been constructed and already received a packet, which
		// can cause problems.
		Thread.sleep(SLEEP * servers.length);
	}
	// creates instances of replicated servers in single JVM. This option
	// should not be used for near-final-testing of fault tolerance because we
	// don't have a way to "crash" servers except in PROCESS_MODE. But
	// PROCESS_MODE can be disabled if you wish to debug graceful mode
	// execution.
	if (!PROCESS_MODE) startReplicatedServersSingleJVM(false);
}


//	/**
//	 * This test tests a simple default DB command expected to always succeed.
//	 *
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	@Test
//	public void test01_DefaultAsync() throws IOException,
//			InterruptedException {
//		client.send(DEFAULT_SADDR, "select table_name from system_schema" + ""
//				+ ".tables");
//	}
//
//	/**
//	 * Tests that a table is indeed being created successfully.
//	 *
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	@Test
//	public void test02_Single_CreateTable_Async() throws IOException,
//			InterruptedException {
//		testCreateTable(true, true);
//	}
//
//
//	@Test
//	public void test03_InsertRecords_Async() throws IOException,
//			InterruptedException {
//		for (int i = 0; i < 10; i++) {
//			send("insert into " + TABLE + " (ssn, firstname, lastname) " +
//					"values (" + (int) (Math.random() * Integer.MAX_VALUE) +
//					", '" + "John" + i + "', '" + "Smith" + i + "')", true);
//		}
//		Thread.sleep(SLEEP);
//		ResultSet resultSet = session.execute("select count(*) from " + TABLE);
//		Assert.assertTrue(!resultSet.isExhausted());
//		Assert.assertEquals(10, resultSet.one().getLong(0));
//	}
//
//	@Test
//	public void test04_DeleteRecords_Async() throws IOException,
//			InterruptedException {
//		send("truncate users", true);
//		Thread.sleep(SLEEP);
//		ResultSet resultSet = session.execute("select count(*) from " + TABLE);
//		Assert.assertTrue(!resultSet.isExhausted());
//		Assert.assertEquals(0, resultSet.one().getLong(0));
//	}
//
//	@Test
//	public void test05_CreateTable_Sync() throws IOException,
//			InterruptedException {
//		testCreateTable(true, false);
//	}
//

/*********** Replicated server tests start below *************/

/**
 * Create tables on all keyspaces. Table should always exist after this
 * test. This test should always succeed, it is irrelevant to the total
 * order implementation.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
public void test10_CreateTables() throws IOException, InterruptedException {
	createEmptyTables();
	Thread.sleep(SLEEP);

	for (String node : servers) {
		verifyTableExists(DEFAULT_TABLE_NAME, node, true);
	}
}

/**
 * Select a single server and send all SQL queries to the selected server.
 * Then verify the results on all replicas to see whether they are
 * consistent.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
public void test11_UpdateRecord_SingleServer() throws IOException,
		InterruptedException {
	// generate a random key for this test
	int key = ThreadLocalRandom.current().nextInt();

	String selected = servers[0];
	// insert a record first with an empty list
	client.send(serverMap.get(selected), insertRecordIntoTableCmd(key,
			DEFAULT_TABLE_NAME));
	Thread.sleep(SLEEP);

	// The choice of servers.length number of requests is just arbitrary
	// and has no significance. Could equally well be say 10. The
	// requests are all being sent to the same server.
	for (int i = 0; i < servers.length; i++) {
		client.send(serverMap.get(selected), updateRecordOfTableCmd(key,
				DEFAULT_TABLE_NAME));
		Thread.sleep(SLEEP);
	}

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}

/**
 * Send a simple SQL query to every server in a round robin manner. Then
 * verify the results in all replicas to see whether they are consistent.
 *
 * @throws IOException
 * @throws InterruptedException
 */
@Test
public void test12_UpdateRecord_AllServer() throws IOException,
		InterruptedException {
	// generate a random key for this test
	int key = ThreadLocalRandom.current().nextInt();

	// insert a record first with an empty list, it doesn't matter which
	// server we use, it should be consistent
	client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key,
			DEFAULT_TABLE_NAME));
	Thread.sleep(SLEEP);

	for (String node : servers) {
		client.send(serverMap.get(node), updateRecordOfTableCmd(key,
				DEFAULT_TABLE_NAME));
		Thread.sleep(SLEEP);
	}

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}

/**
 * Send each SQL query to a random server. Then verify the results in all
 * replicas to see whether they are consistent.
 *
 * @throws InterruptedException
 * @throws IOException
 */
@Test
public void test13_UpdateRecord_RandomServer() throws InterruptedException,
		IOException {
	// generate a random key for this test
	int key = ThreadLocalRandom.current().nextInt();

	// insert a record first with an empty list, it doesn't matter which
	// server we use, it should be consistent
	client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key,
			DEFAULT_TABLE_NAME));
	Thread.sleep(SLEEP);

	for (int i = 0; i < servers.length; i++) {
		String node = servers[ThreadLocalRandom.current().nextInt(0,
				servers.length)];
		client.send(serverMap.get(node), updateRecordOfTableCmd(key,
				DEFAULT_TABLE_NAME));
		Thread.sleep(SLEEP);
	}

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}


/**
 * This test is the same as test13, but it will send update request faster
 * than test13, as it only sleeps 10ms
 *
 * @throws InterruptedException
 * @throws IOException
 */
@Test
public void test14_UpdateRecordFaster_RandomServer() throws InterruptedException, IOException {
	// generate a random key for this test
	int key = ThreadLocalRandom.current().nextInt();

	// insert a record first with an empty list, it doesn't matter which
	// server we use, it should be consistent
	client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key,
			DEFAULT_TABLE_NAME));
	// this sleep is to guarantee that the record has been created
	Thread.sleep(SLEEP);

	for (int i = 0; i < servers.length; i++) {
		String node = servers[ThreadLocalRandom.current().nextInt(0,
				servers.length)];
		client.send(serverMap.get(node), updateRecordOfTableCmd(key,
				DEFAULT_TABLE_NAME));
		// we just sleep 10 milliseconds this time
		Thread.sleep(10);
	}

	Thread.sleep(SLEEP);
	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}


/**
 * This test is also the same as test13, but it will send update request
 * much faster than test13, as it only sleeps 1ms
 *
 * @throws InterruptedException
 * @throws IOException
 */
@Test
public void test15_UpdateRecordMuchFaster_RandomServer() throws InterruptedException, IOException {
	// generate a random key for this test
	int key = ThreadLocalRandom.current().nextInt();

	// insert a record first with an empty list, it doesn't matter which
	// server we use, it should be consistent
	client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key,
			DEFAULT_TABLE_NAME));
	// this sleep is to guarantee that the record has been created
	Thread.sleep(SLEEP);

	for (int i = 0; i < servers.length; i++) {
		String node = servers[ThreadLocalRandom.current().nextInt(0,
				servers.length)];
		client.send(serverMap.get(node), updateRecordOfTableCmd(key,
				DEFAULT_TABLE_NAME));
		// we just sleep 10 milliseconds this time
		Thread.sleep(1);
	}
	Thread.sleep(SLEEP);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}

/**
 * This test will not sleep and send more requests (i.e., 10)
 *
 * @throws InterruptedException
 * @throws IOException
 */
@Test
public void test16_UpdateRecordFastest_RandomServer() throws InterruptedException, IOException {
	// generate a random key for this test
	int key = ThreadLocalRandom.current().nextInt();

	// insert a record first with an empty list, it doesn't matter which
	// server we use, it should be consistent
	client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key,
			DEFAULT_TABLE_NAME));
	// this sleep is to guarantee that the record has been created
	Thread.sleep(SLEEP);

	for (int i = 0; i < NUM_REQS; i++) {
		String node = servers[ThreadLocalRandom.current().nextInt(0,
				servers.length)];
		client.send(serverMap.get(node), updateRecordOfTableCmd(key,
				DEFAULT_TABLE_NAME));

	}

	Thread.sleep(SLEEP * NUM_REQS / SLEEP_RATIO);

	verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
}

public static void main(String[] args) throws IOException {
	addShutdownHook();
	Result result = JUnitCore.runClasses(GraderConsistency.class);
}
}
