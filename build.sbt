organization := "com.vast"

name := "athena"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.11.2", "2.10.4")

description := "A fully nonblocking and asynchronous client library for Cassandra."

// Force compilation in java 1.6
javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6")

resolvers += Resolver.typesafeRepo("releases")

def akka(artifact: String) = "com.typesafe.akka" %% ("akka-" + artifact) % "2.3.5"

libraryDependencies ++= Seq(
  akka("actor"),
  "com.typesafe.play" %% "play-json" % "2.3.3",
  "com.typesafe.play" %% "play-iteratees" % "2.3.3",
  "com.typesafe" % "config" % "1.2.1",
  "com.chuusai" %% "shapeless" % "1.2.4",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  akka("slf4j") % "test",
  akka("testkit") % "test"
)

CassandraUtils.cassandraTestSettings
