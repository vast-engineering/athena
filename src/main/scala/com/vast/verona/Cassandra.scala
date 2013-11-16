package com.vast.verona

import akka.actor._
import com.typesafe.config.Config
import akka.io.Tcp
import java.net.InetSocketAddress

import com.vast.verona.client.{ConnectorSettings, ClientSettings}
import spray.util.actorSystem

object Cassandra extends ExtensionKey[CassandraExt] {

  //commands
  type Command = Tcp.Command

  /**
   * A command to initiate a single connection to a single Cassandra node.
   */
  case class Connect(remoteAddress: InetSocketAddress,
                     settings: Option[ClientSettings] = None) extends Command
  object Connect {
    def apply(host: String, port: Int, settings: Option[ClientSettings]): Connect = {
      val address = new InetSocketAddress(host, port)
      Connect(address, settings)
    }
  }

  /**
   * A command to initiate a managed connection pool to a single Cassandra node
   */
  case class NodeConnectorSetup(remoteAddress: InetSocketAddress,
                                settings: Option[ConnectorSettings] = None) extends Command {
    private[verona] def normalized(implicit refFactory: ActorRefFactory) =
      if (settings.isDefined) this
      else copy(settings = Some(ConnectorSettings(actorSystem)))
  }
  
  object NodeConnectorSetup {
    def apply(host: String, port: Int, settings: Option[ConnectorSettings])(implicit refFactory: ActorRefFactory): NodeConnectorSetup =
      NodeConnectorSetup(new InetSocketAddress(host, port), settings).normalized
  }

  /**
   * A command to initiate a connection to a cluster of Cassandra nodes
   *
   */
  case class ClusterConnectorSetup(hosts: Seq[InetSocketAddress], settings: Option[ConnectorSettings] = None) extends Command

  type CloseCommand = Tcp.CloseCommand
  val Close = Tcp.Close
  val ConfirmedClose = Tcp.ConfirmedClose
  val Abort = Tcp.Abort

  case class CloseAll(kind: CloseCommand) extends Command
  object CloseAll extends CloseAll(Close)


  /// events
  type Event = Tcp.Event

  type Connected = Tcp.Connected; val Connected = Tcp.Connected
  type ConnectionClosed = Tcp.ConnectionClosed

  val Closed = Tcp.Closed
  val Aborted = Tcp.Aborted
  val ConfirmedClosed = Tcp.ConfirmedClosed
  val PeerClosed = Tcp.PeerClosed
  type ErrorClosed = Tcp.ErrorClosed; val ErrorClosed = Tcp.ErrorClosed

  case object ClosedAll extends Event

  type CommandFailed = Tcp.CommandFailed; val CommandFailed = Tcp.CommandFailed

  case class NodeConnectorInfo(nodeConnector: ActorRef, setup: NodeConnectorSetup)
  case class ClusterConnectorInfo(clusterConnector: ActorRef, setup: ClusterConnectorSetup)

}

class CassandraExt(system: ExtendedActorSystem) extends akka.io.IO.Extension {

  val Settings = new Settings(system.settings.config.getConfig("com.vast.verona"))
  class Settings private[CassandraExt](config: Config) {
    val ManagerDispatcher = config.getString("manager-dispatcher")
    val SettingsGroupDispatcher = config.getString("settings-group-dispatcher")
    val ConnectionDispatcher = config getString "connection-dispatcher"

  }

  val manager = system.actorOf(
    props = Props[CassandraManager](new CassandraManager(Settings)) withDispatcher Settings.ManagerDispatcher,
    name = "IO-Cassandra")

}

