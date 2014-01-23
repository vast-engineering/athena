package athena.client

import akka.actor._
import akka.io.IO
import akka.pattern._
import akka.util.{Timeout, ByteString}
import akka.actor.Status.Failure
import akka.event.{Logging, LoggingAdapter}

import athena.{SerialConsistency, Consistency, Athena}

import play.api.libs.iteratee.Enumerator

import athena.data.CValue
import athena.Consistency.Consistency
import athena.SerialConsistency.SerialConsistency
import athena.Athena.{ClusterConnectorInfo, AthenaException}
import athena.Requests.SimpleStatement

import java.util.concurrent.TimeUnit
import java.net.InetAddress
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.concurrent.{ExecutionContext, Future}
import spray.util.LoggingContext
import athena.client.Pipelining.Pipeline

trait Session {

  /**
   * Execute a query and return an asynchronous stream of result rows.
   */
  def executeQuery(query: String,
                   values: Seq[CValue] = Seq(),
                   keyspace: Option[String] = None,
                   routingKey: Option[ByteString] = None,
                   consistency: Consistency = Consistency.ONE,
                   serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Enumerator[Row]

  def executeUpdate(query: String,
                   values: Seq[CValue] = Seq(),
                   keyspace: Option[String] = None,
                   routingKey: Option[ByteString] = None,
                   consistency: Consistency = Consistency.ONE,
                   serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Future[Unit]


  /**
   * This method must be called to properly dispose of the Session.
   */
  def close()
}

object Session {

  private val defaultTimeoutDuration = FiniteDuration(5, TimeUnit.SECONDS)
  private implicit val defaultTimeout = Timeout(defaultTimeoutDuration)
 //private implicit def getLogger(implicit actorRefFactory: ActorSystem): LoggingAdapter = Logging.getLogger(actorRefFactory, classOf[Session])

  def apply(connection: ActorRef)
           (implicit log: LoggingContext, ec: ExecutionContext): Session = {
    new SimpleSession(Pipelining.pipeline(connection)) {
      /**
       * This method must be called to properly dispose of the Session.
       */
      override def close() {
        //do nothing
      }
    }
  }

  /**
   * Create a session using an existing ActorSystem
   */
  def apply(initialHosts: Set[InetAddress], port: Int, keyspace: Option[String] = None, 
            waitForConnection: Boolean = true)(implicit system: ActorRefFactory): Session = {
    import system.dispatcher
    val connection = getConnection(initialHosts, port, keyspace, system, waitForConnection)
    val pipe = Pipelining.pipeline(connection)
    new SimpleSession(pipe) {
      def close(): Unit = {
        //shutdown our actor
        connection.foreach(system.stop)
      }
    }
  }

  private def getConnection(hosts: Set[InetAddress], port: Int,
                            keyspace: Option[String] = None, actorRefFactory: ActorRefFactory,
                            waitForConnect: Boolean = true): Future[ActorRef] = {
    import actorRefFactory.dispatcher
    val connectTimeout = if(waitForConnect) defaultTimeoutDuration else Duration.Inf
    val setup = Athena.ClusterConnectorSetup(hosts, port, keyspace, None)
    val initializer = actorRefFactory.actorOf(Props(new ConnectorInitializer(connectTimeout)))
    initializer.ask(setup).mapTo[ActorRef]
  }

  private class ConnectorInitializer(connectTimeout: Duration) extends Actor with ActorLogging {

    def receive: Receive = {
      case x: Athena.ConnectionCreationCommand =>
        IO(Athena)(context.system) ! x
        val connectCommander = sender
        context.become {
          case Athena.CommandFailed(_) =>
            connectCommander ! Failure(new AthenaException("Could not create connector."))
            context.stop(self)

          case evt: Athena.ConnectionCreatedEvent =>
            log.debug("Got cluster connection actor")
            val connector = sender
            context.setReceiveTimeout(connectTimeout)
            context.become {
              case connected: Athena.ConnectedEvent =>
                log.debug("Connector initialized - returning connector")
                connectCommander ! connector
                context.stop(self)

              case ReceiveTimeout =>
                log.warning("Timed out waiting for cluster to connect.")
                connector ! Athena.Abort
                connectCommander ! Failure(new AthenaException("Timed out waiting for cluster to connect."))
                context.stop(self)
            }

        }
    }
  }

  private abstract class SimpleSession(pipeline: Pipeline)
                              (implicit log: LoggingContext, ec: ExecutionContext) extends Session {

    private[this] val queryPipeline = Pipelining.queryPipeline(pipeline)
    private[this] val updatePipeline = Pipelining.updatePipeline(pipeline)

    def executeQuery(query: String,
                     values: Seq[CValue] = Seq(),
                     keyspace: Option[String] = None,
                     routingKey: Option[ByteString] = None,
                     consistency: Consistency = Consistency.ONE,
                     serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Enumerator[Row] = {
      queryPipeline(SimpleStatement(query, values, keyspace, routingKey, Some(consistency), Some(serialConsistency)))
    }

    def executeUpdate(query: String,
                      values: Seq[CValue] = Seq(),
                      keyspace: Option[String] = None,
                      routingKey: Option[ByteString] = None,
                      consistency: Consistency = Consistency.ONE,
                      serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Future[Unit] = {
      updatePipeline(SimpleStatement(query, values, keyspace, routingKey, Some(consistency), Some(serialConsistency)))
    }

  }

}

