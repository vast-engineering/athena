import SonatypeKeys._

sonatypeSettings

credentials := Seq(Credentials(Path.userHome / ".ivy2" / ".sonatype-credentials"))

profileName := "com.vast"

// To sync with Maven central, you need to supply the following information:
pomExtra := {
  <url>https://github.com/vast-engineering/athena</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:github.com/vast-engineering/athena</connection>
    <developerConnection>scm:git:git@github.com:vast-engineering/athena</developerConnection>
    <url>https://github.com/vast-engineering/athena</url>
  </scm>
  <developers>
    <developer>
      <id>dpratt@vast.com</id>
      <name>David Pratt</name>
      <url>https://github.com/vast-engineering</url>
    </developer>
  </developers>
}