package com.vast.verona.client

import com.vast.verona.util.SettingsBase
import com.typesafe.config.Config

case class ConnectorSettings(poolSettings: PoolSettings, clientSettings: ClientSettings)

object ConnectorSettings extends SettingsBase[ConnectorSettings]("com.vast.verona.connector") {
  def fromSubConfig(c: Config): ConnectorSettings = {
    apply(
      PoolSettings.fromSubConfig(c.getConfig("pool")),
      ClientSettings.fromSubConfig(c.getConfig("client"))
    )
  }
}

case class PoolSettings(
  localCoreConnections: Int,
  localMaxConnections: Int,
  remoteCoreConnections: Int,
  remoteMaxConnections: Int,
  localMinConcurrentRequests: Int,
  localMaxConcurrentRequests: Int,
  remoteMinConcurrentRequests: Int,
  remoteMaxConcurrentRequests: Int)

object PoolSettings extends SettingsBase[PoolSettings]("com.vast.verona.connector.pool") {
  def fromSubConfig(c: Config): PoolSettings = {
    apply(
      c.getInt("local-core-connections"),
      c.getInt("local-max-connections"),
      c.getInt("remote-core-connections"),
      c.getInt("remote-max-connections"),
      c.getInt("local-min-concurrent-requests"),
      c.getInt("local-max-concurrent-requests"),
      c.getInt("remote-min-concurrent-requests"),
      c.getInt("remote-max-concurrent-requests")
    )
  }
}

