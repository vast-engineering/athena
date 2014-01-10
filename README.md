# Athena

This is a fully asynchronous and nonblocking Cassandra client built on top of the (Excellent) Akka-IO framework.

Currently, it can create a fault tolerant pool of load balanced connections to a Cassandra cluster, execute queries and marshal results.

What doesn't work yet -

* Authentication
* Connection compression
* Prepared statements
* Changing keyspaces
* Intelligent (key aware) query routing



