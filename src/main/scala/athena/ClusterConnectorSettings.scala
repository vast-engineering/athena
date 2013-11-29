package athena

import spray.util._

import athena.util.SettingsBase
import com.typesafe.config.Config

case class ClusterConnectorSettings(localNodeSettings: NodeConnectorSettings, remoteNodeSettings: NodeConnectorSettings)

object ClusterConnectorSettings extends SettingsBase[ClusterConnectorSettings]("athena.cluster-connector") {
  def fromSubConfig(c: Config): ClusterConnectorSettings = {
    ClusterConnectorSettings(
      NodeConnectorSettings.fromSubConfig(c.getConfig("local-nodes")),
      NodeConnectorSettings.fromSubConfig(c.getConfig("remote-nodes"))
    )
  }
}
