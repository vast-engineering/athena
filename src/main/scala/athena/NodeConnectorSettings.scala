package athena

import athena.util._
import com.typesafe.config.Config
import scala.concurrent.duration.Duration

case class NodeConnectorSettings(closeTimeout: Duration, poolSettings: PoolSettings, connectionSettings: ConnectionSettings)

object NodeConnectorSettings extends SettingsBase[NodeConnectorSettings]("athena.node-connector") {
  def fromSubConfig(c: Config): NodeConnectorSettings = {
    apply(
      c.getDuration("close-timeout"),
      PoolSettings.fromSubConfig(c.getConfig("pool")),
      ConnectionSettings.fromSubConfig(c.getConfig("connection"))
    )
  }
}

case class PoolSettings(connectionTimeout: Duration,
                        coreConnections: Int,
                        maxConnections: Int,
                        minConcurrentRequests: Int,
                        maxConcurrentRequests: Int)

object PoolSettings extends SettingsBase[PoolSettings]("athena.node-connector.pool") {
  def fromSubConfig(c: Config): PoolSettings = {
    apply(
      c.getDuration("connection-timeout"),
      c.getInt("core-connections"),
      c.getInt("max-connections"),
      c.getInt("min-concurrent-requests"),
      c.getInt("max-concurrent-requests")
    )
  }
}

