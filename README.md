**USING CONSENSUS TO BUILD DISTRIBUTED SYSTEMS: Part 2**

[TOC]

# Overview #

### Goal ###
The goal of this assignment is to use consensus to build a fault-tolerant replicated datastore application using two of the following three options (third for optional extra credit):

1. **Replicated state machine** (GigaPaxos): GigaPaxos to encapsulate the application as an RSM;

2. **Coordination server** (Zookeeper): A coordination protocol using Zookeeper as a logically centralized service accessible to all replicas;

3. **Custom protocol (optional extra credit)** (Custom): Use an approach other than the above two to make your system fault-tolerant. Here are some suggestions: 1) Use an alternative consensus algorithm like [Raft](https://raft.github.io/) (many java implementations available online) in your server implementation; 2) Implement your own fault-tolerant-consensus-based server-to-server protocol that is good enough to pass the tests. Note: Option #2 is not easy, but you may be able to do a good enough job to pass the tests without a complete or efficient implementation.

### Prerequisites ###

1. Java is required; unix is strongly recommended but not necessary for any of the three options above;

2. Familiarity with the [replicated consistent (non-fault-tolerant) datastore programming assignment (PA2)](https://bitbucket.org/distrsys/consistent-db/src/master/);

3. Completion of [consensus and RSM tutorials (Part 1)](https://bitbucket.org/distrsys/consensus-rsm-tutorials/src/master/README.md?mode=edit&at=master).

You are already familiar with the application environment and background here having completed prerequisite #2 above. 

The goal in this assignment is to make your previous replicated, linearizable consistent, non-fault-tolerant datastore fault-tolerant now using at least two of the three options listed in Goal above.

***

#  File organization #

### Your code files ###
Your work will use exactly one (not both) of the two files in the `faulttolerance` package as a starting point for your implementation:

1. [`MyDBReplicableAppGP`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/src/server/faulttolerance/MyDBReplicableAppGP.java) if using the GigaPaxos/RSM approach.

2. [`MyDBFaultTolerantServerZK`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/src/server/faulttolerance/MyDBFaultTolerantServerZK.java) otherwise (including the optional extra-credit approach).

You may create as many additional code files as needed in the `faulttolerance` package to support your implementation. 
***

### Test code files ###
The test code (what used to be [`Grader`] for consistency-only tests) has evolved a fair bit for fault tolerance as follows:

1. [`GraderCommonSetup`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/test/GraderCommonSetup.java): Common setup code for both consistency and fault tolerance tests.

2. [`GraderConsistency`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/test/GraderConsistency.java): Tests same as the old consistency tests in [`Grader`] (https://bitbucket.org/distrsys/consistent-db/src/master/test/Grader.java).  

3. [`GraderFaultTolerance`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/test/GraderFaultTolerance.java): Setup and tests for testing fault tolerance, the primary testing focus of this assignment, inherited from `GraderCommonSetup`. The documentation of the tests in this class should be self-explanatory.

4. [`ServerFailureRecoveryManager`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/test/ServerFailureRecoveryManager.java): A class with sadistic (oops, I meant static) utiility methods that derive pleasure from killing and restarting your servers to stress-test fault tolerance.

***

# Constraints #
Your implementation must respect the following constraints, but these are not meant to be exhaustive, so if in doubt, ask.

1. Pick exactly one of the three high-level options at a time in the Goal section above; do not mix multiple options. 

2. If using the Zookeeper option, keep in mind the following (also documented at the top of [`MyDBFaultTolerantServerZK`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/src/server/faulttolerance/MyDBFaultTolerantServerZK.java)):
	1. You can not use any other form of coordination (like the file system or a database) between servers other than through Zookeeper. 
	2. The constraint above also precludes using the backend datastore itself for coordination, thus your server can not write to or read from any keyspace other than the one specified in its [constructor](https://bitbucket.org/distrsys/fault-tolerant-db/src/e4247b90b8e5fce088791db1d254ec4b217f66e1/src/server/faulttolerance/MyDBFaultTolerantServerZK.java#lines-60).
	3. You can assume that a single Zookeeper server is running on `localhost` at the [default port](https://bitbucket.org/distrsys/fault-tolerant-db/src/8d5714f278fd658e80f6f541a7e30cf0714e3500/src/server/faulttolerance/MyDBFaultTolerantServerZK.java#lines-49) when your submission is graded.
	
3. For all options, you can assume that a single Cassandra instance is running at the address specified in [MyDBFaultTolerantServerZK#main](https://bitbucket.org/distrsys/fault-tolerant-db/src/9a12b86469508854d641de52f19170ec6db712b5/src/server/faulttolerance/MyDBFaultTolerantServerZK.java#lines-107).

4. For all options, you can not maintain any in-memory or on-disk data structure containing more than [`MAX_LOG_SIZE (default 400)`](https://bitbucket.org/distrsys/fault-tolerant-db/src/9a12b86469508854d641de52f19170ec6db712b5/src/server/faulttolerance/MyDBFaultTolerantServerZK.java#lines-49) requests.

5. You shouldn't need any external libraries not already included, but you are welcome to check with us and use external libraries that would make your implementation less onerous whille preserving the spirit of the assignmennt.
	
	***

# Getting started #

Start by running your consistency-only replicated server (or using the [sample solution](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/src/server/AVDBReplicatedServer.java) with [STUDENT_TESTING_MODE`=false`](https://bitbucket.org/distrsys/fault-tolerant-db/src/9a12b86469508854d641de52f19170ec6db712b5/test/GraderCommonSetup.java#lines-93)) by running GraderConsistency. You should see the old consistency-only tests pass. 

Next, revert `STUDENT_TESTING_MODE` to its default `true`, and verify that tests in [`GraderFaultTolerance`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/test/GraderFaultTolerance.java) fail. These tests fail because both `MyDBFaultTolerantServerZK` and `MyDBReplicableApp` throw "unimplemented" runtime exceptions, which you can see by disabling the default `PROCESS_MODE=true` flag so everything runs in a single JVM.

From here on, you need to read the documentation of each test, understand why it's failing, and take it from there to make your replicated server consistent and fault-tolerant. The documentation and test method names should be self-explanatory.

***

# Submission instructions #

1. Submit a Bitbucket or Github repository forked from this repository to Gradescope keeping in mind the following:

    1. Only files in the `faulttolerance` package should contain your source changes. Any changes to any other source or test files will be ignored.
	2. Include detailed documentation in your code files and follow good coding practices (helpful names, minimal privilege, thoughtfully handling exceptions beyond just printing a stack trace, defensive testing, effective use of loggers instead of print statements, etc.) 
	3. A design document (up to but not necessarily 3 pages long) 
		* explaining your design(s), wherein the plural is for if you choose to implement two separate options for extra credit; 
		* explicitly noting how many tests passed in your testing; 
		* conceptual explanation for failing tests, i.e., understand and explain why the test is failing (possibly including sample output) and what you think you need to implement to bug-fix or complete your design.
	
***


# Tips, troubleshooting, FAQs #
1. In addition to the inline source documentation, there are handy tips in [`test/README.txt`](https://bitbucket.org/distrsys/fault-tolerant-db/src/master/test/README.txt) for playing with various testing/debugging options.
2. You don't need any source code for either Zookeeper or Gigapaxos, nevertheless you are strongly encouraged to download the sources into your IDE so that you can easily browse through the documentation of available methods as well as use debuggers more effectively.
3. You don't need any binaries other than the ones already included.
4. You can do this assignment on Windows as well as it does not rely on any Gigapaxos or Zookeeper shell scripts, only Java.
5. The RSM option is probably the fewest lines of code followed by Zookeeper followed by the Custom option, however this ordering may not necessarily correspond to the amount of time you might spend getting those options to work.
6. As always, the tests or config files provided are not intended to be exhaustive, and we may test your code with more stressful tests or configurations.
7. Always remember to clear all state before every test run (e.g., paxos_logs/ in the GigaPaxos/RSM option and any files/tables/znodes you may have created in the other two options), otherwise you may be potentially carrying over bugs from previous runs.
8. Always remember that a crash fault-tolerant consistent system must not make any changes to state without going through some kind of a consensus protocol, otherwise you will invariably end up violating safety.
9. It is okay to assume that all application state will be contained in just the one table to which `Grader*` writes.

More based on your FAQs.

