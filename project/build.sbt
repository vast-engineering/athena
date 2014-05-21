libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "org.slf4j" % "slf4j-nop" % "1.7.7",
  "org.cassandraunit" % "cassandra-unit" % "2.0.2.1" excludeAll ExclusionRule(organization = "org.slf4j")
)
