libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-compress" % "1.8.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.4", //for setting up the test instance
  "com.vast" %% "sbt-slf4j" % "0.1.0"
)

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.2.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.3")

