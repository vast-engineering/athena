organization := "com.vast"

name := "athena"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.10.4"

scalacOptions += "-target:jvm-1.7"

licenses += "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")

description := "A fully nonblocking and asynchronous client library for Cassandra."

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "spray repo" at "http://repo.spray.io"

def akka(artifact: String) = "com.typesafe.akka" %% ("akka-" + artifact) % "2.3.3"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.1",
  akka("actor"),
  "com.typesafe.play" %% "play-iteratees" % "2.2.3",
  "com.typesafe.play" %% "play-json" % "2.2.3",
  "io.spray" % "spray-util" % "1.3.1",
  "commons-lang" % "commons-lang" % "2.6",
  "com.chuusai" %% "shapeless" % "1.2.4",
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  akka("slf4j") % "test",
  akka("testkit") % "test"
)




