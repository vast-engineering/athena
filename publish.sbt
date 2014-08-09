import com.typesafe.sbt.SbtPgp
import xerial.sbt.Sonatype.SonatypeKeys._

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/vast-engineering"))

startYear := Some(2013)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/vast-engineering/sbt-slf4j"),
    "scm:git:github.com/vast-engineering/athena",
    Some("scm:git:git@github.com:vast-engineering/athena")
  )
)

lazy val SonatypePublish = config("sonatype")

inConfig(SonatypePublish)(
  Defaults.baseClasspaths ++
    Seq(
      credentials += Credentials(Path.userHome / ".ivy2" / ".sonatype-credentials"),
      pomExtra := {
        <developers>
          <developer>
            <id>dpratt@vast.com</id>
            <name>David Pratt</name>
            <url>https://github.com/vast-engineering</url>
          </developer>
        </developers>
      }
    ) ++
    SbtPgp.settings ++
    sonatypeSettings :+ (profileName := "com.vast")
)