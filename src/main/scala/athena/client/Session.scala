package athena.client

import athena.{SerialConsistency, Consistency, Athena, ClusterConnectorSettings}
import akka.actor.{ActorRef, ActorSystem}

import akka.io.IO
import akka.pattern._
import akka.util.{Timeout, ByteString}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import java.net.InetAddress
import athena.Requests.SimpleStatement
import play.api.libs.iteratee.Enumerator
import scala.concurrent.{ExecutionContext, Future}
import athena.data.CValue
import athena.Consistency.Consistency
import athena.SerialConsistency.SerialConsistency
import akka.event.{Logging, LoggingAdapter}

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

  private val defaultTimeoutDuration = Duration(5, TimeUnit.SECONDS)
  private implicit val defaultTimeout = Timeout(defaultTimeoutDuration)
  private implicit def getLogger(implicit system: ActorSystem): LoggingAdapter = Logging.getLogger(system, classOf[Session])

  /**
   * Create a session using an existing ActorSystem
   */
  def apply(initialHosts: Set[InetAddress], port: Int, keyspace: Option[String] = None)(implicit system: ActorSystem): Session = {
    import system.dispatcher
    val connection = getConnection(initialHosts, port, keyspace, system)
    new SimpleSession(connection) {
      def close(): Unit = {
        //shutdown our actor
        connection.foreach(system.stop)
      }
    }
  }

  private def getConnection(hosts: Set[InetAddress], port: Int, keyspace: Option[String] = None, system: ActorSystem): Future[ActorRef] = {
    import system.dispatcher
    for {
      Athena.ClusterConnectorInfo(connection, _) <- IO(Athena)(system).ask(Athena.ClusterConnectorSetup(hosts, port, keyspace, None))
    } yield {
      connection
    }
  }

  private abstract class SimpleSession(connection: Future[ActorRef])(implicit log: LoggingAdapter, ec: ExecutionContext) extends Session {

    private[this] val pipeline = Pipelining.pipeline(connection)
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

