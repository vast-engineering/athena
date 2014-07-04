import sbt.Keys._
import sbt._
import xerial.sbt.Sonatype._
import xerial.sbt.Sonatype.SonatypeKeys._

object SonatypePublish {

  lazy val Sonatype = config("sonatype")

  lazy val sonatypePublishSettings = inConfig(Sonatype)(
    sonatypeSettings ++
      Seq(
        credentials := Seq(Credentials(Path.userHome / ".ivy2" / ".sonatype-credentials")),
        profileName := "com.vast",
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
      )
  )


}