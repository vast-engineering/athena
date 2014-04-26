name := "Athena"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

scalacOptions += "-target:jvm-1.7"

organization := "com.vast"

licenses += "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")

description := "A fully nonblocking and asynchronous client library for Cassandra."

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "spray repo" at "http://repo.spray.io"

def akka(artifact: String) = "com.typesafe.akka" %% ("akka-" + artifact) % "2.3.0"

libraryDependencies ++= Seq(akka("actor"), akka("slf4j"), akka("testkit") % "test" )

libraryDependencies ++= Seq(
	"com.typesafe" % "config" % "1.2.0",
	"com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
	"com.typesafe.play" %% "play-iteratees" % "2.2.2",
	"com.typesafe.play" %% "play-json" % "2.2.2",
	"io.spray" % "spray-util" % "1.3.1",
	"commons-lang" % "commons-lang" % "2.6",
	"com.chuusai" %% "shapeless" % "1.2.4",
	"org.scalatest" %% "scalatest" % "2.0" % "test",
	"org.slf4j" % "slf4j-api" % "1.7.5",
	"org.slf4j" % "slf4j-jdk14" % "1.7.5"   
)


