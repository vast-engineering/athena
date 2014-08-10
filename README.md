# Athena

This is a fully asynchronous and nonblocking Cassandra client built on top of the (Excellent) Akka-IO framework.

Currently, it can create a fault tolerant pool of load balanced connections to a Cassandra cluster, stream queries and marshal results.

What doesn't work yet -

* Authentication
* Connection compression
* Intelligent (key aware) query routing
* Documentation - sample code

## Using Athena

[sonatype]: https://oss.sonatype.org/index.html

Binary release artifacts for Scala 2.11.x and 2.10.x are published to the [Sonatype Maven Repository][sonatype]
and synced to Maven Central. The current release is only available as a SNAPSHOT (for now), but the final 0.3.0
release is imminent.

### SBT builds
```scala
libraryDependencies ++= Seq(
  "com.vast" %% "athena" % "0.3.0-SNAPSHOT"
)
```

### Maven builds
```xml
<properties>
    <scala.major.version>2.10</scala.major.version>
</properties>

<dependencies>
    <dependency>
        <groupId>com.vast</groupId>
        <artifactId>athena_${scala.major.version}</artifactId>
        <version>0.3.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Usage

TODO: More info here

### Actors

The high level interface for Athena is through the ClusterConnector actor. This actor models a pool of connections
to all the hosts in your cluster, and will load balance queries across them in an appropriate fashion.

### Sessions

There is currently an experimental higher level Session abstraction that models interactions as a series of pipelines,
where a pipeline is simply defined as

```scala
AthenaRequest => Future[AthenaResponse]
```

A Session wraps a pipeline and adds more useful methods on top of it, and also the ability to stream rows back using
Play Enumerators. The entirety of this API, however, is really just a prototype and is subject to change across versions
with little to no warning.

## Building
Athena is built with SBT 0.13.x, and is designed to be essentially and out of the box build. You should be able to
compile, package and (if needed) deploy locally by simply doing 'sbt publishLocal' from the command line. The
test suite requires an active instance of cassandra to run against. Fortunately, the SBT harness is built in such a way
that it automatically handles downloading the proper version of cassandra, configuring it, starting the server
and setting up a test keyspace before the tests run. This instance is then torn down after tests complete. Most of this
functionality is configured using SBT settings - look in project/CassandraUtils.scala for info on how to change it.
For example, if you wish to disable the server auto-provisioning to use your own instance, just add

```scala
import CassandraUtils.Keys._

provisionServer := false
```

to your local.sbt file.
