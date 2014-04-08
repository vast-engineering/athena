package athena

import akka.actor._

import com.typesafe.config.Config
import java.net.{InetAddress, InetSocketAddress}

import Consistency._

import spray.util.actorSystem
import athena.connector.CassandraError

object Athena extends ExtensionKey[AthenaExt] {

  /**
   * An object describing an error.
   */
  trait Error {
    def message: String
    def toThrowable: Throwable
  }

  case class GeneralError(message: String) extends Error {
    def toThrowable: Throwable = new AthenaException(message)
  }

  case class ConnectionError(host: String, port: Int) extends Error {
    def message: String = s"Connection to $host:$port failed."
    def toThrowable: Throwable = new AthenaException(message)
  }

  //base trait for all messages to and from these actors
  trait Message

  //base trait for all commands
  trait Command extends Message

  /**
   * A Command that can be sent to the extension itself.
   */
  sealed trait ExtensionCommand extends Command

  sealed trait ConnectionCreationCommand extends ExtensionCommand

  /**
   * A command to initiate a single connection to a single Cassandra node.
   */
  case class Connect(remoteAddress: InetSocketAddress,
                     keyspace: Option[String],
                     settings: Option[ConnectionSettings],
                     eventHandler: Option[ActorRef]) extends ConnectionCreationCommand
  object Connect {
    def apply(host: String, port: Int, keyspace: Option[String], settings: Option[ConnectionSettings] = None): Connect = {
      val address = new InetSocketAddress(host, port)
      Connect(address, keyspace, settings, None)
    }
  }

  /**
   * A command to initiate a managed connection pool to a single Cassandra node.
   *
   * @param remoteAddress The host and port to connect to.
   * @param keyspace The optional keyspace to use for connections in the pool.
   * @param settings Settings for the pool - if this is not defined, default settings read from the ActorSystem's config will be used.
   */
  case class NodeConnectorSetup(remoteAddress: InetSocketAddress,
                                keyspace: Option[String],
                                settings: Option[NodeConnectorSettings] = None) extends ConnectionCreationCommand {
    private[athena] def normalized(implicit refFactory: ActorRefFactory) =
      if (settings.isDefined) {
        this
      } else {
        copy(settings = Some(NodeConnectorSettings(actorSystem)))
      }
  }

  object NodeConnectorSetup {
    def apply(host: String, port: Int,
              keyspace: Option[String],
              settings: Option[NodeConnectorSettings])
              (implicit refFactory: ActorRefFactory): NodeConnectorSetup =
      NodeConnectorSetup(new InetSocketAddress(host, port), keyspace, settings).normalized
  }

  /**
   * A command to initiate a connection to a cluster of Cassandra nodes.
   *
   * @param initialHosts A list of host addressed used to seed the cluster.
   * @param port the port used to connect
   * @param keyspace The optional keyspace to use for connections in the cluster.
   * @param settings Settings for the cluster - if this is not defined, default settings read from the ActorSystem's config will be used.
   * @param failOnInit If true, the cluster conntection will fail at initialization time if no hosts are reachable. If false,
   *                   it will continue to attempt reconnects.
   */
  case class ClusterConnectorSetup(initialHosts: Set[InetAddress], port: Int, keyspace: Option[String],
                                   settings: Option[ClusterConnectorSettings] = None, failOnInit: Boolean = false) extends ConnectionCreationCommand {
    private[athena] def normalized(implicit refFactory: ActorRefFactory) =
      if (settings.isDefined) this
      else copy(settings = Some(ClusterConnectorSettings(actorSystem)))
  }

  //Send to a cluster connector actor to monitor (or stop monitoring) it's state
  case object AddClusterStatusListener extends Command
  case object RemoveClusterStatusListener extends Command

  /**
   * A command suitable for routing to a connection, host or cluster
   */
  trait ConnectionCommand extends Command

  /**
   * Common interface for all commands which aim to close down an open connection, host or cluster
   */
  sealed trait CloseCommand extends ConnectionCommand {
    /**
     * The corresponding event which is sent as an acknowledgment once the
     * close operation is finished.
     */
    def event: ConnectionClosed
  }

  /**
   * A normal close operation. This will mark the connection as closed, and any new commands
   * will be rejected. Outstanding responses will be generated, and once complete, the connection
   * will be closed in an orderly fashion.
   */
  case object Close extends CloseCommand {
    /**
     * The corresponding event which is sent as an acknowledgment once the
     * close operation is finished.
     */
    override def event = Closed
  }
  /**
   * Immediately terminate the connection, and return error responses to any outstanding requests
   */
  case object Abort extends CloseCommand {
    /**
     * The corresponding event which is sent as an acknowledgment once the
     * close operation is finished.
     */
    override def event = Aborted
  }

  /**
   * Commands suitable for a node connector
   */
  sealed trait NodeCommand

  /**
   * Instruct a node connector to immediately attempt a reconnection (if disconnected)
   */
  case object Reconnect extends NodeCommand

  /**
   * Instruct a node connector to disconnect it's pool. Reconnection attempts will be scheduled.
   */
  case object Disconnect extends NodeCommand


  /// events
  trait Event extends Message

  sealed trait ConnectionCreatedEvent extends Event

  /**
   * The connection actor sends this message to the sender of a [[athena.Athena.ClusterConnectorSetup]]
   * command. The connection is characterized by the `remoteAddress`
   * and `localAddress` TCP endpoints.
   */
  case class Connected(remoteAddress: InetSocketAddress, localAddress: InetSocketAddress) extends ConnectionCreatedEvent

  /**
   * Sent on successful creation of a node or cluster connector.
   */
  case class NodeConnectorInfo(nodeConnector: ActorRef, setup: NodeConnectorSetup) extends ConnectionCreatedEvent
  case class ClusterConnectorInfo(clusterConnector: ActorRef, setup: ClusterConnectorSetup) extends ConnectionCreatedEvent


  /**
   * This is the common interface for all events which indicate that a connection
   * has been closed
   */
  sealed trait ConnectionClosed extends Event {
    /**
     * `true` iff the connection has been closed in response to an [[athena.Athena.Abort]] command.
     */
    def isAborted: Boolean = false
    /**
     * `true` iff the connection was closed by the server.
     */
    def isServerClosed: Boolean = false
    /**
     * `true` iff the connection has been closed due to an IO error.
     */
    def isErrorClosed: Boolean = false
    /**
     * If `isErrorClosed` returns true, then the error condition can be
     * retrieved by this method.
     */
    def getErrorCause: String = null
  }

  /**
   * The connection has been closed normally in response to a [[athena.Athena.Close]] command.
   */
  case object Closed extends ConnectionClosed
  /**
   * The connection has been aborted in response to an [[athena.Athena.Abort]] command.
   */
  case object Aborted extends ConnectionClosed {
    override def isAborted = true
  }
  /**
   * The peer has closed its writing half of the connection.
   */
  case object ServerClosed extends ConnectionClosed {
    override def isServerClosed = true
  }
  /**
   * The connection has been closed due to an IO error.
   */
  case class ErrorClosed(cause: String) extends ConnectionClosed {
    override def isErrorClosed = true
    override def getErrorCause = cause
  }

  case object ClosedAll extends Event

  /**
   * Whenever a command cannot be completed, the queried actor will reply with
   * this message, wrapping the original command which failed.
   */
  case class CommandFailed(cmd: Command, error: Option[Error] = None) extends Event

  /**
   * Events emitted by a node connector.
   */
  sealed trait NodeStatusEvent extends Event
  case class NodeConnected(host: InetAddress) extends NodeStatusEvent
  case class NodeFailed(host: InetAddress, error: Error) extends NodeStatusEvent
  case class NodeDisconnected(host: InetAddress) extends NodeStatusEvent

  sealed trait ClusterStatusEvent extends Event
  case object ClusterDisconnected extends ClusterStatusEvent
  case object ClusterConnected extends ClusterStatusEvent
  case class ClusterFailed(error: Error) extends ClusterStatusEvent

  //exceptions
  class AthenaException(message: String, cause: Option[Throwable]) extends RuntimeException(message, cause.orNull) {
    def this(message: String) { this(message, None) }
    def this(cause: Throwable) { this(null, Some(cause)) }
    def this() { this(null, None) }
  }

  /**
   * Thrown when the connector experiences an unrecoverable error, or has reached an inconsistent state. This exception
   * normally signifies an internal implementation problem or bug.
   */
  class InternalException(message: String, cause: Option[Throwable]) extends AthenaException(message, cause) {
    def this(message: String) { this(message, None) }
  }

  class ConnectionException(message: String) extends AthenaException(message)
  class AuthenticationException(message: String, host: InetSocketAddress) extends ConnectionException(s"Authentication error on host $host: $message")

//  class ConnectionAttemptFailedException(val host: String, val port: Int) extends ConnectionException(s"Connection attempt to $host:$port failed")

  class NoHostAvailableException(message: String, val errors: Map[InetAddress, Any]) extends AthenaException(message)

  class QueryExecutionException(message: String) extends AthenaException(message)
  class UnavailableException(message: String, val consistency: Consistency, val required: Int, val alive: Int)
    extends QueryExecutionException(s"Not enough replica available for query at consistency $consistency ($required required but only $alive alive): $message")
  class TruncateException(message: String) extends QueryExecutionException(message)

  class QueryTimeoutException(message: String) extends QueryExecutionException(message)

  class WriteTimeoutException(message: String, val consistency: Consistency, val writeType: String, val received: Int, val required: Int)
    extends QueryTimeoutException(s"Cassandra timeout during write query at consistency $consistency ($required replica were required but only $received acknowledged the write): $message")

  class ReadTimeoutException(message: String, val consistency: Consistency, val received: Int, val required: Int, val dataPresent: Boolean)
    extends QueryTimeoutException(ReadTimeoutException.createMessage(message, consistency, received, required, dataPresent))
  private[this] object ReadTimeoutException {
    def createMessage(message: String, consistency: Consistency, received: Int, required: Int, dataPresent: Boolean): String = {

      val suffix = if (received < required)
        s"$required responses were required but only %$received replicas responded"
      else if (!dataPresent)
        "the replica queried for data didn't respond"
      else
        "timeout while waiting for repair of inconsistent replica"


      s"Cassandra timeout during read query at consistency $consistency ($suffix): $message"
    }
  }

  class QueryValidationException(message: String) extends AthenaException(message)
  class InvalidQuerySyntaxException(message: String) extends QueryValidationException(message)
  class UnauthorizedException(message: String) extends QueryValidationException(message)
  class InvalidQueryException(message: String) extends QueryValidationException(message)
  class InvalidConfigurationInQueryException(message: String) extends InvalidQueryException(message)

  class AlreadyExistsException(message: String, val keyspace: String, val table: String) extends QueryValidationException(AlreadyExistsException.createMessage(message, keyspace, table))
  object AlreadyExistsException {
    def createMessage(message: String, keyspace: String, table: String) = {
      if (table.isEmpty)
        s"Keyspace $keyspace already exists: $message"
      else
        s"Table $keyspace.$table already exists: $message"
    }
  }
}



class AthenaExt(system: ExtendedActorSystem) extends akka.io.IO.Extension {

  val Settings = new Settings(system.settings.config.getConfig("athena"))
  class Settings private[AthenaExt](config: Config) {
  }

  val manager = system.actorOf(props = Props[AthenaManager](new AthenaManager(Settings)), name = "IO-Athena")

}

