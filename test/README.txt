Class organization:

GraderCommonSetup: Parent class of GraderConsistency that has all the setup needed to start
student servers before starting tests and close them gracefully after.

GraderConsistency: Has the tests for non-fault-tolerant replicated total-order-consistent
servers.

GraderFaultTolerance: Checks for fault tolerance, also inherited from
GraderCommonSetup, but it can also be inherited from GraderConsistency if you want to have all
tests, both without and with fault-tolerance, to run in one fell swoop.

ServerFailureRecoveryManager: Has the necessary utility methods to start or kill
 servers to simulate different fault scenarios. You can also use its methods to
 further stress test your implementation.

Tips:

+ Multi-process vs. single JVM: You can enable/disable the PROCESS_MODE flag in
GraderCommonSetup to run each replica as a separate process or as a reflectively
instantiated object by GraderConsistency . PROCESS_MODE is default and ensures
that you can't "abuse" static variables (that are global variables in Java) to
share distributed information without distributed message-passing. Disabling
PROCESS_MODE may be easier for debugging (but with the caveat that step-through
debuggers are poor for concurrency/distributedness because they change the very
thing being observed in a Heisenbergian mannner).

+ Zookeeper vs. Gigapaxos: The GIGAPAXOS_MODE in GraderCommonSetup means Zookeeper mode,
not Gigapaxos mode. You have to fix one or the other for an implementation
because they two have different designs, the former being a coordination object
storage service and the latter being an RSM.

+ Logging: Use logging effectively or learn to do so. The default log properties
 file is logging.properties.

+ Output redirection: If you want to observe the standard output/error of
different servers in separate files, set REDIRECT_IO_TO_FILE=true. But again,
learning to use logging is a much more effective technique rather than using
print statements for debugging.

+ The flag STUDENT_MODE should always be true for you. When it is false, the
instructor solutions will be used for development and testing of instructor
solution implementation. For example, STUDENT_MODE=false will pass all tests in
GraderConsistency by using the provided AVDBReplicatedServer implementation.

+ All requests to Gigapaxos-based servers will be RequestPacket requests, so you
 don't need to worry about creating your own packet types that are visible to
 and explicitly registered with gigapaxos.


Troubleshooting:

