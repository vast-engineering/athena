package athena

import scala.concurrent.duration.Duration

import com.typesafe.config.Config
import athena.util._
import Consistency._
import SerialConsistency.SerialConsistency

/**
 * Models connector settings for connections to Cassandra hosts. Contains options dealing with options such as
 * protocol compression, query defaults, and low-level TCP settings.
 *
 */
case class ConnectionSettings(requestTimeout: Duration, querySettings: QuerySettings, socketSettings: SocketSettings)

object ConnectionSettings extends SettingsBase[ConnectionSettings]("athena.connection") {
  def fromSubConfig(c: Config): ConnectionSettings = {
    ConnectionSettings(
      c.getDuration("request-timeout"),
      QuerySettings.fromSubConfig(c.getConfig("query")),
      SocketSettings.fromSubConfig(c.getConfig("socket"))
    )
  }
}

case class QuerySettings(
  defaultConsistencyLevel: Consistency,
  defaultSerialConsistencyLevel: SerialConsistency,
  defaultFetchSize: Int) {
  require(0 < defaultFetchSize, "fetch-size must be > 0")
}

object QuerySettings extends SettingsBase[QuerySettings]("athena.connection.query") {
  def fromSubConfig(c: Config): QuerySettings = {
    apply(
      Consistency.withName(c.getString("default-consistency-level")),
      SerialConsistency.withName(c.getString("default-serial-consistency-level")),
      c.getInt("default-fetch-size")
    )
  }
}

case class SocketSettings (
  connectTimeout: Duration,
  readTimeout: Duration
//  keepAlive: Boolean,
//  reuseAddress: Boolean,
//  soLinger: Int,
//  tcpNoDelay: Boolean,
//  receiveBufferSize: Int,
//  sendBufferSize: Int) {
                            ) {

  require(connectTimeout > Duration.Zero, "connect-timeout must be greater than zero.")
  require(readTimeout > Duration.Zero, "read-timeout mus tbe greater than zero.")
//  require(0 < soLinger, "so-linger must be > 0")
//  require(0 < receiveBufferSize, "receive-buffer-size must be > 0")
//  require(0 < sendBufferSize, "send-buffer-size must be > 0")
}

object SocketSettings extends SettingsBase[SocketSettings]("athena.connector.socket"){
  def fromSubConfig(c: Config): SocketSettings = {
    apply(
      c.getDuration("connect-timeout"),
      c.getDuration("read-timeout")
//      c.getBoolean("keep-alive"),
//      c.getBoolean("reuse-address"),
//      c.getInt("so-linger"),
//      c.getBoolean("tcp-no-delay"),
//      c.getInt("receive-buffer-size"),
//      c.getInt("send-buffer-size")
    )
  }

}
