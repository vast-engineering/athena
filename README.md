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