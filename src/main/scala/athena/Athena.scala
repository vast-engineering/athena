package athena

import akka.actor._

import com.typesafe.config.Config
import java.net.InetSocketAddress

import athena.data.Consistency._

import spray.util.actorSystem

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
    def toThrowable: Throwable = new ConnectionAttemptFailedException(host, port)
  }

  //base trait for all messages to and from these actors
  trait Message

  //base trait for all commands
  trait Command extends Message

  /**
   * A Command that can be sent to the extension itself.
   */
  trait ExtensionCommand extends Command

  /**
   * A command to initiate a single connection to a single Cassandra node.
   */
  case class Connect(remoteAddress: InetSocketAddress,
                     keyspace: Option[String],
                     settings: Option[ConnectionSettings]) extends ExtensionCommand
  object Connect {
    def apply(host: String, port: Int, keyspace: Option[String], settings: Option[ConnectionSettings] = None): Connect = {
      val address = new InetSocketAddress(host, port)
      Connect(address, keyspace, settings)
    }
  }

  /**
   * A command to initiate a managed connection pool to a single Cassandra node
   */
  case class NodeConnectorSetup(remoteAddress: InetSocketAddress,
                                keyspace: Option[String],
                                settings: Option[NodeConnectorSettings] = None) extends ExtensionCommand {
    private[athena] def normalized(implicit refFactory: ActorRefFactory) =
      if (settings.isDefined) this
      else copy(settings = Some(NodeConnectorSettings(actorSystem)))
  }
  
  object NodeConnectorSetup {
    def apply(host: String, port: Int,
              keyspace: Option[String],
              settings: Option[NodeConnectorSettings])(implicit refFactory: ActorRefFactory): NodeConnectorSetup =
      NodeConnectorSetup(new InetSocketAddress(host, port), keyspace, settings).normalized
  }

  /**
   * A command to initiate a connection to a cluster of Cassandra nodes
   */
  case class ClusterConnectorSetup(name: String, initialHosts: Set[InetSocketAddress], settings: Option[ClusterConnectorSettings] = None) extends ExtensionCommand {
    private[athena] def normalized(implicit refFactory: ActorRefFactory) =
      if (settings.isDefined) this
      else copy(settings = Some(ClusterConnectorSettings(actorSystem)))
  }


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

  case class CloseAll(kind: CloseCommand) extends ConnectionCommand
  object CloseAll extends CloseAll(Close)


  /// events
  trait Event extends Message

  /**
   * The connection actor sends this message to the sender of a [[athena.Athena.Connect]]
   * command. The connection is characterized by the `remoteAddress`
   * and `localAddress` TCP endpoints.
   */
  case class Connected(remoteAddress: InetSocketAddress, localAddress: InetSocketAddress) extends Event

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
  case class CommandFailed(cmd: Command) extends Event

  case class NodeConnectorInfo(nodeConnector: ActorRef, setup: NodeConnectorSetup)
  case class ClusterConnectorInfo(clusterConnector: ActorRef, setup: ClusterConnectorSetup)


  //exceptions
  class AthenaException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
    def this(message: String) { this(message, null) }
    def this(cause: Throwable) { this(null, cause) }
    def this() { this(null, null) }
  }

  /**
   * Thrown when the connector experiences an unrecoverable error, or has reached an inconsistent state. This exception
   * normally signifies an internal implementation problem or bug.
   */
  class InternalException(message: String, cause: Throwable) extends AthenaException(message, cause) {
    def this(message: String) { this(message, null) }
  }

  class ConnectionException(message: String) extends AthenaException(message)
  class AuthenticationException(message: String, host: InetSocketAddress) extends ConnectionException(s"Authentication error on host $host: $message")

  class ConnectionAttemptFailedException(val host: String, val port: Int) extends ConnectionException(s"Connection attempt to $host:$port failed")

//  class RequestTimeoutException(val request: CassandraRequest, message: String)
//    extends AthenaException(message)

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

