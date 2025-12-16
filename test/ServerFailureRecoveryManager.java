import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.utils.Util;
import org.junit.Assert;
import server.MyDBReplicatedServer;
import server.ReplicatedServer;
import server.faulttolerance.MyDBFaultTolerantServerZK;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;


/**
 * This class has mostly static utility methods in order to help simulate
 * various failure and recovery scenarios for servers. These include failing or
 * recovering one server at a time, multiple servers sequentially, and multiple
 * servers concurrently at random times.
 */
public class ServerFailureRecoveryManager {
protected static final Logger log =
		Logger.getLogger(ServerFailureRecoveryManager.class.getName());
private static Map<String, Process> serverPIDs = new ConcurrentHashMap<>();
private static ProcessBuilder processBuilder = new ProcessBuilder().inheritIO();

/* specify the java command path here if the getJavaCommand()
method below doesn't work as expected.
 */
public static final String JAVA_PATH = null;

protected final static ScheduledExecutorService scheduler =
		Executors.newScheduledThreadPool(1);


private static String getJavaCommand() {
	if (JAVA_PATH != null) return JAVA_PATH;
	String java_command;
	if (System.getProperty("os.name").startsWith("Win")) {
		java_command =
				System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java.exe";
	}
	else {
		java_command =
				System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java";
	}
	return java_command;
}

private static String getClassPath() {
	return System.getProperty("java.class.path");
}

private static List<String> getServerStartCommand() {
	return getServerStartCommand(true);
}

private static List<String> getServerStartCommand(boolean modeFT) {
	List<String> list = new ArrayList<String>();
	list.add(getJavaCommand());
	list.add("-cp");
	list.add(getClassPath());
	list.add("-DgigapaxosConfig=conf/gigapaxos.properties");
	list.add("-Djava.util.logging.config.file=logging.properties");
	list.add(GraderConsistency.STUDENT_TESTING_MODE ?

			(modeFT ? (!GraderFaultTolerance.GIGAPAXOS_MODE ?
					MyDBFaultTolerantServerZK.class.getName() :
					ReconfigurableNode.class.getName()) :
					// student replicated consistency implementation
					MyDBReplicatedServer.class.getName())

			: (modeFT ? (!GraderFaultTolerance.GIGAPAXOS_MODE ? ("server" +
			".AVDBFaultTolerantServerZK") :
			ReconfigurableNode.class.getName()) :
			// instructor replicated consistency implementation
			"server.AVDBReplicatedServer"));

	if (!modeFT || !GraderFaultTolerance.GIGAPAXOS_MODE)
		list.add(new File(GraderConsistency.CONFIG_FILE).getAbsolutePath());


	return list;
}

private static List<String> getServerStartCommand(String node, boolean modeFT) {
	List<String> cmd = getServerStartCommand(modeFT);
	if (modeFT && GraderFaultTolerance.GIGAPAXOS_MODE) {
		cmd.add(node);
		cmd.add("start");
	}

	cmd.add(node);

	String s = "";
	for (String t : cmd) s = s + " " + t;
	System.out.println("Executing start server command: " + s);
	return cmd;
}


private static Process startProcess(List<String> commandArgs, String node) throws IOException {
	File file = new File(node + ".out");
	return GraderCommonSetup.REDIRECT_IO_TO_FILE ?
			processBuilder.redirectOutput(file).redirectError(file).directory(new File(System.getProperty("user.dir"))).command(commandArgs).start()

			:

			processBuilder.inheritIO().command(commandArgs).start();
	//Runtime.getRuntime().exec(commandArgs.toArray(new String[0]));
}

private Map<String, InetSocketAddress> reconfigurators = new HashMap<String,
		InetSocketAddress>();


/*************** Start of synchronized start/kill primitive methods */

private synchronized static Process startServer(String node) throws IOException, InterruptedException {
	return startServer(node, true);
}

// Starts a server by starting it as a separate process
private synchronized static Process startServer(String node, boolean modeFT) throws IOException, InterruptedException {

	Process p;
	// server already running
	if (serverPIDs.containsKey(node) && (p = serverPIDs.get(node)).isAlive())
		return p;

	// else start server
	System.out.println("Recovering " + node);
	// gigapaxos apps needs to start with a clean slate as the app will
	// be recovered from a recent checpoint and requests there onnwards
	// rolled forward
	//if (GraderFaultTolerance.GIGAPAXOS_MODE)
		GraderCommonSetup.clearTable(node);
	serverPIDs.put(node, (p = startProcess(getServerStartCommand(node, modeFT)
			, node)));
	return p;
}

/* Same as startServer, explicitly noted to document that "start" and
"recover" mean the same thing, and will be no-ops if the server is
found to be already running.
 */
protected synchronized static Process recoverServer(String node) throws IOException, InterruptedException {
	// recover server implies FT mode
	return startServer(node, true);
}


/*
Return value's non-nullness indicates if the server was actually killed.
 */
protected synchronized static String killServer(String node) {
	Process p;
	// server already running
	if (serverPIDs.containsKey(node) && serverPIDs.get(node).isAlive()) {
		serverPIDs.remove(node).destroyForcibly();
		return node;
	}
	return null;
}

// Explicitly kill if running and then start
protected synchronized static Process restartServer(String node) throws IOException, InterruptedException {
	killServer(node);
	return startServer(node, true);
}

// convenience method
protected synchronized static void killAllServers() {
	for (Process p : serverPIDs.values())
		p.destroyForcibly();
	serverPIDs.clear();
}

// Just a more apt alias :)
protected synchronized static void mercilesslySlaughterAll() {
	killAllServers();
}

protected synchronized static void startAllServers() throws IOException,
		InterruptedException {
	startAllServers(true);
}

// convenience method
protected synchronized static void startAllServers(boolean modeFT) throws IOException, InterruptedException {
	for (String node : GraderCommonSetup.nodeConfigServer.getNodeIDs()) {
		startServer(node, modeFT);
	}
	System.out.println("Started all servers; serverProcesses=" + serverPIDs);
}

/*************** Start of synchronized start/kill primitive methods */


// Kills a randomly selected server and returns the killed server
protected static String killRandomServer() throws IOException {
	return killServer(getRandomServer());
}


protected static Set<String> killRandomServers(int numServers) throws IOException {
	Set<String> hitList = getRandomServers(numServers);
	for (String node : hitList)
		killServer(node);
	return hitList;
}

/*
Crashes a random server and recovers it after millis ms.
 */
protected static void killAndRecoverRandomServer(long millis) throws IOException, InterruptedException {
	recoverServer(killRandomServer(), millis);
}

// schedule server recovery after millis
protected static void recoverServer(String server, long millis) throws IOException, InterruptedException {

	scheduler.schedule(new Runnable() {
		public void run() {
			try {
				startServer(server, true);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}, millis, TimeUnit.MILLISECONDS);
}

// scheduled version of killServer
protected static void killServer(String server, long millis) throws IOException, InterruptedException {

	scheduler.schedule(new Runnable() {
		public void run() {
			killServer(server);
		}
	}, millis, TimeUnit.MILLISECONDS);
}

// Kill numServers distinct servers now and recover them millis later
protected static void killAndRecoverRandomServers(int numServers,
												  long millis) throws IOException, InterruptedException {
	Set<String> hitList = new HashSet<String>();
	while (hitList.size() < numServers)
		// ensure distinct random servers
		while (!hitList.add(killRandomServer())) ;
	for (String server : hitList)
		recoverServer(server, millis);

}

// utility method to add some variance
private static long perturb(long millis, double fractionalPerturbation) {
	return (long) (millis * ((1 - fractionalPerturbation) + Math.random() * fractionalPerturbation * 2));
}

// default variance of 50%
private static long perturb(long millis) {
	return perturb(millis, 0.5);
}

protected static double PERTURB_FRAC = 0.5;

/* Serial killer version of killRandomServers, which kills numServers
servers sequentially with a mean interkill time of millis.
 */
protected static Set<String> serialKill(int numServers, long interKillMillis) throws IOException, InterruptedException {
	Set<String> hitList = getRandomServers(numServers);
	long fate = 0;
	for (String victim : hitList)
		killServer(victim, (fate = fate + perturb(interKillMillis,
				PERTURB_FRAC)));
	return hitList;
}

protected static void serialRecover(Set<String> recoverList, long startTime,
									long interRecoveryTime) throws IOException
		, InterruptedException {
	long fate = startTime;
	for (String victim : recoverList)
		recoverServer(victim, (fate = fate + perturb(interRecoveryTime,
				PERTURB_FRAC)));
}

protected static void serialKillAndThenSerialRecover(int numServers,
													 long killInterval,
													 long recoverInterval) throws IOException, InterruptedException {
	serialRecover(serialKill(numServers, killInterval),
			(long) (killInterval * (1 + PERTURB_FRAC) * numServers),
			recoverInterval);
}

/**
 * MTTF is mean time to failure and MTTR is mean time to recovery.
 */
static class KillRecover implements Callable<KillRecover> {

	final String server;
	final long mttf;
	final long mttr;
	static boolean stop = true;

	KillRecover(String server, long mttf, long mttr) {
		this.server = server;
		this.mttf = mttf;
		this.mttr = mttr;
	}

	@Override
	public KillRecover call() throws Exception {
		if (stop) return this;
		if (isAlive(server)) {
			killServer(server);
			scheduler.schedule(this, perturb(mttr), TimeUnit.MILLISECONDS);
		}
		else {
			startServer(server, true);
			scheduler.schedule(this, perturb(mttf), TimeUnit.MILLISECONDS);
		}
		return this;
	}
}

protected static void startRandomFailureRecovery(long mttf, long mttr) {
	KillRecover.stop = false;
	for (String server : GraderCommonSetup.nodeConfigServer.getNodeIDs())
		scheduler.schedule(new KillRecover(server, mttf, mttr), perturb(mttf),
				TimeUnit.MILLISECONDS);
}

protected static void stopRandomFailureRecovery(long timeout) throws InterruptedException {
	KillRecover.stop = true;
	scheduler.awaitTermination(timeout, TimeUnit.MILLISECONDS);
}

private static int getNumAlive() {
	return serverPIDs.size();
}

private static Set<String> getAliveServers() {
	return serverPIDs.keySet();
}

protected static String getRandomServer() {
	String[] servers =
			GraderCommonSetup.nodeConfigServer.getNodeIDs().toArray(new String[0]);
	return servers[(int) (Math.random() * servers.length)];
}

private static Set<String> getRandomServers(int numServers) {
	Set<String> hitList = new HashSet<String>();
	while (hitList.size() < numServers)
		// ensure distinct random servers
		hitList.add((String) Util.getRandomOtherThan(GraderCommonSetup.nodeConfigServer.getNodeIDs(), hitList));
	return hitList;
}


private static boolean isAlive(String server) {
	return serverPIDs.containsKey(server) && serverPIDs.get(server).isAlive();
}

private static void checkAssertsEnabled() {
	try {
		Assert.assertTrue(false);
	} catch (Error e) {
		return;
	}
	throw new RuntimeException("Asserts not enabled");
}

// code to test ourself, i.e., the methods above.
public static void main(String[] args) throws IOException,
		InterruptedException {

	try {
		checkAssertsEnabled();
		NodeConfig<String> nodeConfigServer =
				NodeConfigUtils.getNodeConfigFromFile(GraderConsistency.CONFIG_FILE, ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET);
		String[] servers =
				nodeConfigServer.getNodeIDs().toArray(new String[0]);
		GraderCommonSetup.nodeConfigServer = nodeConfigServer;

		System.out.println("Java command: " + getJavaCommand());
		System.out.println("Start server command: " + getServerStartCommand());

		System.out.println("Starting " + servers[0] + " with output/error" +
				" being redirected to " + (GraderCommonSetup.REDIRECT_IO_TO_FILE ? servers[0] + ".out" : "stdout/stderr"));
		Process p = startServer(servers[0]);
		System.out.println("Killing " + servers[0]);
		p.destroyForcibly();
		long t = System.nanoTime();
		while (p.isAlive()) ;
		System.out.println("Time since kill and isAlive=false = " + ((System.nanoTime() - t) / 1000.0 + "us"));

		System.out.println("Starting all servers...");
		startAllServers();
		for (String server : servers)
			assert (isAlive(server));
		System.out.println("Started all servers");

		assert (getNumAlive() == servers.length);

		killRandomServer();
		assert (getNumAlive() == servers.length - 1);
		startAllServers();
		killRandomServers(2);
		assert (getNumAlive() == servers.length - 2);
		startAllServers();

		serialKillAndThenSerialRecover(2, 500, 300);
		assert (getNumAlive() == servers.length);

		startRandomFailureRecovery(500, 600);
		t = System.currentTimeMillis();
		while (System.currentTimeMillis() < t + 5000) {
			System.out.println(getNumAlive() + " out of " + nodeConfigServer.getNodeIDs().size() + " servers " + "alive");
			Thread.sleep(250);
		}
		stopRandomFailureRecovery(1000);
		System.out.println(getNumAlive() + " out of " + nodeConfigServer.getNodeIDs().size() + " servers alive " + "at the end of " + "random " + "" + "" + "failure " + "" + "" + "recovery " + "test");


		Thread.sleep(1000);
	} finally {
		killAllServers();
		scheduler.shutdownNow();
		GraderCommonSetup.cluster.close();
	}
}
}
