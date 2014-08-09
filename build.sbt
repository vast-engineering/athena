organization := "com.vast"

name := "athena"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.11.2", "2.10.4")

description := "A fully nonblocking and asynchronous client library for Cassandra."

resolvers += Resolver.typesafeRepo("releases")

resolvers += "spray repo" at "http://repo.spray.io"

def akka(artifact: String) = "com.typesafe.akka" %% ("akka-" + artifact) % "2.3.4"

def spray(artifact: String) = "io.spray" %% artifact % "1.3.1"

libraryDependencies ++= Seq(
  akka("actor"),
  //spray("spray-util"),
  "com.typesafe.play" %% "play-json" % "2.3.2",
  "com.typesafe.play" %% "play-iteratees" % "2.3.2",
  "com.typesafe" % "config" % "1.2.1",
  "commons-lang" % "commons-lang" % "2.6",
  "com.chuusai" %% "shapeless" % "1.2.4",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  akka("slf4j") % "test",
  akka("testkit") % "test"
)

CassandraUtils.cassandraTestSettings
