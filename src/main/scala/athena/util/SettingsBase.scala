package athena.util

import akka.actor.ActorSystem
import com.typesafe.config.Config

abstract class SettingsBase[T](prefix: String) {

  def apply(system: ActorSystem): T = apply(system.settings.config)
  def apply(config: Config): T = fromSubConfig(config getConfig prefix)

  def fromSubConfig(c: Config): T
}