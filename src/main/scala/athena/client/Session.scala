package athena.client

import akka.actor._
import akka.io.IO
import akka.pattern._
import akka.util.Timeout
import akka.actor.Status.Failure

import athena.{SerialConsistency, Consistency, Athena}

import play.api.libs.iteratee.Enumerator

import athena.data.CValue
import athena.Consistency.Consistency
import athena.SerialConsistency.SerialConsistency
import athena.Athena.AthenaException
import athena.Requests.SimpleStatement

import java.util.concurrent.TimeUnit
import java.net.InetAddress
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.concurrent.{ExecutionContext, Future}
import spray.util.LoggingContext
import athena.client.pipelining.Pipeline
import athena.connector.ClusterConnector

trait Session {

  /**
   * Execute a query and return an asynchronous stream of result rows. Note, this method will 
   * defer actual execution of the query until the Enumerator has an Iteratee attached to it.
   */
  def executeStream(query: String,
                    values: Seq[CValue] = Seq(),
                    consistency: Consistency = Consistency.ONE,
                    serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Enumerator[Row]

  /**
   * Execute a query intended to update data. As opposed to the method above, this method 
   * executes the query immediately. This also aggregates any and all result rows into memory. This 
   * avoids the need to stream rows, but be aware that if the query returns a large row count, they will
   * all be buffered in memory.
   */
  def execute(query: String,
              values: Seq[CValue] = Seq(),
              consistency: Consistency = Consistency.ONE,
              serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Future[Seq[Row]]


  /**
   * This method must be called to properly dispose of the Session.
   */
  def close()
}

object Session {

  private val defaultTimeoutDuration = FiniteDuration(45, TimeUnit.SECONDS)
  private implicit val defaultTimeout = Timeout(defaultTimeoutDuration)

  /**
   * Create a session using an already existing (and assumed valid) Connection ActorRef.
   */
  def apply(connection: ActorRef)
           (implicit log: LoggingContext, ec: ExecutionContext): Session = {
    new SimpleSession(pipelining.pipeline(connection)) {
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
    val pipe = pipelining.pipeline(connection)
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
    //import actorRefFactory.dispatcher
    val connectTimeout = if (waitForConnect) defaultTimeoutDuration else Duration.Inf
    val setup = Athena.ClusterConnectorSetup(hosts, port, keyspace, None)
    val initializer = actorRefFactory.actorOf(Props(new ConnectorInitializer(connectTimeout)))
    initializer.ask(setup).mapTo[ActorRef]
  }

  private class ConnectorInitializer(connectTimeout: Duration) extends Actor with ActorLogging {

    def receive: Receive = {
      case x: Athena.ClusterConnectorSetup =>
        IO(Athena)(context.system) ! x
        val connectCommander = sender
        context.become {
          case Athena.CommandFailed(_, error) =>
            connectCommander ! Failure(new AthenaException("Could not create connector.", error.map(_.toThrowable)))
            context.stop(self)

          case evt: Athena.ConnectionCreatedEvent =>
            log.debug("Got cluster connection actor")
            val connector = sender
            context.setReceiveTimeout(connectTimeout)
            context.become {
              case Athena.ClusterConnected =>
                log.debug("Connector initialized - returning connector")
                connectCommander ! connector
                context.stop(self)

              case Athena.ClusterFailed(error) =>
                connectCommander ! Failure(error.toThrowable)
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

  abstract class SimpleSession(pipeline: Pipeline)
                                      (implicit log: LoggingContext, ec: ExecutionContext) extends Session {

    private[this] val queryPipe = pipelining.queryPipeline(pipeline)
    private[this] val streamPipe = pipelining.streamingPipeline(pipeline)

    def executeStream(query: String,
                     values: Seq[CValue] = Seq(),
                     consistency: Consistency = Consistency.ONE,
                     serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Enumerator[Row] = {
      streamPipe(SimpleStatement(query, values, Some(consistency), Some(serialConsistency)))
    }

    def execute(query: String,
                      values: Seq[CValue] = Seq(),
                      consistency: Consistency = Consistency.ONE,
                      serialConsistency: SerialConsistency = SerialConsistency.SERIAL): Future[Seq[Row]] = {
      queryPipe(SimpleStatement(query, values,Some(consistency), Some(serialConsistency)))
    }

  }

}

