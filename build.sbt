organization := "com.vast"

name := "athena"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.11.6", "2.10.5")

description := "A fully nonblocking and asynchronous client library for Cassandra."

// Force compilation in java 1.6
javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6")

resolvers += Resolver.typesafeRepo("releases")

def akka(artifact: String) = "com.typesafe.akka" %% ("akka-" + artifact) % "2.3.11"

def play(artifact: String) = "com.typesafe.play" %% ("play-" + artifact) % "2.3.9"

libraryDependencies ++= Seq(
  akka("actor"),
  play("json"),
  play("iteratees"),
  "com.typesafe" % "config" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  akka("slf4j") % "test",
  akka("testkit") % "test"
)

CassandraUtils.cassandraTestSettings
