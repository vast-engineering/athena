libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-compress" % "1.8.1"
)

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.3")

addSbtPlugin("no.arktekk.sbt" % "aether-deploy" % "0.13")
