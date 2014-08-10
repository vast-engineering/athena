package athena

import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.language.experimental.macros

package object util {
  implicit class ConfigOps(val c: Config) extends AnyVal {
    def getDuration(path: String): Duration = c.getString(path) match {
      case "infinite" ⇒ Duration.Inf
      case x          ⇒ Duration(x)
    }
  }
}
