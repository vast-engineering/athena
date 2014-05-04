package athena.client

import akka.actor._
import akka.io.IO
import akka.pattern._
import akka.util.Timeout
import akka.actor.Status.Failure

import athena.{Responses, Athena}

import play.api.libs.iteratee.Enumerator

import athena.data.{PreparedStatementDef, CVarChar, CValue}
import athena.Consistency.Consistency
import athena.SerialConsistency.SerialConsistency
import athena.Athena.AthenaException
import athena.Requests.{BoundStatement, Prepare, SimpleStatement}

import java.util.concurrent.TimeUnit
import java.net.InetAddress
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.concurrent.{Promise, ExecutionContext, Future}
import spray.util.LoggingContext
import athena.client.pipelining.Pipeline
import athena.util.{LoggingSource, Rate}
import athena.Responses.Rows
import scala.util.control.NonFatal

trait Session {

  /**
   * Execute a query and return an asynchronous stream of result rows. Note, this method will 
   * defer actual execution of the query until the Enumerator has an Iteratee attached to it.
   */
  def executeStream(query: String,
                    values: Seq[CValue] = Seq(),
                    consistency: Option[Consistency] = None,
                    serialConsistency: Option[SerialConsistency] = None): Enumerator[Row]

  def streamPrepared(statement: PreparedStatementDef,
                    values: Seq[CValue] = Seq(),
                    consistency: Option[Consistency] = None,
                    serialConsistency: Option[SerialConsistency] = None): Enumerator[Row]

  /**
   * Execute a query. As opposed to the method above, this method
   * executes the query immediately. This also aggregates any and all result rows into memory. This 
   * avoids the need to stream rows, but be aware that if the query returns a large row count, they will
   * all be buffered in memory.
   */
  def execute(query: String,
              values: Seq[CValue] = Seq(),
              consistency: Option[Consistency] = None,
              serialConsistency: Option[SerialConsistency] = None): Future[Seq[Row]]

  def executePrepared(statement: PreparedStatementDef,
              values: Seq[CValue] = Seq(),
              consistency: Option[Consistency] = None,
              serialConsistency: Option[SerialConsistency] = None): Future[Seq[Row]]

  def prepare(query: String): Future[PreparedStatementDef]

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
  def apply(connection: ActorRef, keyspace: String)
           (implicit logSource: LoggingSource, log: LoggingContext, ec: ExecutionContext): Session = {
    new SimpleSession(pipelining.pipeline(validateKeyspace(connection, keyspace)), keyspace) {
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
  def apply(initialHosts: Set[InetAddress], port: Int, keyspace: String,
            waitForConnection: Boolean = true)(implicit system: ActorRefFactory, log: LoggingContext): Session = {
    import system.dispatcher
    val connection = getConnection(initialHosts, port, keyspace, system, waitForConnection)
    val pipe = pipelining.pipeline(connection)
    new SimpleSession(pipe, keyspace) {
      def close(): Unit = {
        //shutdown our actor
        connection.flatMap { c =>
          akka.pattern.gracefulStop(c, defaultTimeoutDuration, Athena.Close) recover {
            case NonFatal(e) =>
              //hard kill the actor
              log.warning("Athena session did not terminate in time. Killing actor.")
              system.stop(c)
          }
        } andThen {
          case _ => log.info("Athena session terminated.")
        }
      }
    }
  }

  private def validateKeyspace(connection: ActorRef, keyspace: String)(implicit ec: ExecutionContext, log: LoggingContext): Future[ActorRef] = {
    import akka.pattern._

    connection.ask(SimpleStatement("SELECT * FROM schema_keyspaces where keyspace_name = ?", Seq(CVarChar(keyspace)), Some("system"))).map {
      case Rows(_, _, data, _) if data.size == 1 => connection
      case _ =>
        log.error("Could not validate keyspace {}", keyspace)
        throw new AthenaException(s"Invalid keyspace $keyspace")
    }
  }

  private def getConnection(hosts: Set[InetAddress], port: Int,
                            keyspace: String, actorRefFactory: ActorRefFactory,
                            waitForConnect: Boolean = true): Future[ActorRef] = {
    import actorRefFactory.dispatcher
    val connectTimeout = if (waitForConnect) defaultTimeoutDuration else Duration.Inf
    val setup = Athena.ClusterConnectorSetup(hosts, port, None)
    val initializer = actorRefFactory.actorOf(Props(new ConnectorInitializer(connectTimeout)))
    initializer.ask(setup).mapTo[ActorRef].flatMap(c => validateKeyspace(c, keyspace))
  }

  private class ConnectorInitializer(connectTimeout: Duration) extends Actor with ActorLogging {

    def receive: Receive = {
      case x: Athena.ClusterConnectorSetup =>
        IO(Athena)(context.system) ! x
        val connectCommander = sender()
        context.become {
          case Athena.ClusterConnectorInfo(connector, _) =>
            log.debug("Got cluster connection actor")
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

          case unexpectedResponse =>
            log.error("Got unexpected response to cluster creation - {}", unexpectedResponse)
            connectCommander ! Failure(new AthenaException("Could not create connector."))
            context.stop(self)

        }
    }
  }

  abstract class SimpleSession(pipeline: Pipeline, keyspace: String)
                                      (implicit logSource: LoggingSource, log: LoggingContext, ec: ExecutionContext) extends Session {

    private[this] val queryLog = logSource("query.SimpleSession")
    private[this] val queryPipe = pipelining.queryPipeline(pipeline)
    private[this] val streamPipe = pipelining.streamingPipeline(pipeline)

    def executeStream(query: String,
                     values: Seq[CValue] = Seq(),
                     consistency: Option[Consistency] = None,
                     serialConsistency: Option[SerialConsistency] = None): Enumerator[Row] = {
      queryLog.info("Streaming query {} with params {}", query, values)
      val rate = new Rate
      streamPipe(SimpleStatement(query, values, Some(keyspace), consistency, serialConsistency)).onDoneEnumerating {
        queryLog.info(" Finished streaming query {} with params {} {}", query, values, rate)
      }
    }

    def streamPrepared(statement: PreparedStatementDef,
                      values: Seq[CValue] = Seq(),
                      consistency: Option[Consistency] = None,
                      serialConsistency: Option[SerialConsistency] = None): Enumerator[Row] = {
      val bound = BoundStatement(statement, values, None, consistency, serialConsistency)
      queryLog.info("Streaming prepared query {} with params {}", statement.rawQuery, values)
      val rate = new Rate
      streamPipe(bound).onDoneEnumerating {
        queryLog.info(" Finished streaming prepared query {} with params {} {}", statement.rawQuery, values, rate)
      }
    }

    def execute(query: String,
                values: Seq[CValue] = Seq(),
                consistency: Option[Consistency] = None,
                serialConsistency: Option[SerialConsistency] = None): Future[Seq[Row]] = {
      queryLog.info("Executing query {} with params {}", query, values)
      val rate = new Rate
      queryPipe(SimpleStatement(query, values, Some(keyspace), consistency, serialConsistency)).andThen {
        case _ => queryLog.info(" Executed query {} with params {} {}", query, values, rate)
      }
    }

    def executePrepared(statement: PreparedStatementDef,
                values: Seq[CValue] = Seq(),
                consistency: Option[Consistency] = None,
                serialConsistency: Option[SerialConsistency] = None): Future[Seq[Row]] = {
      val bound = BoundStatement(statement, values, None, consistency, serialConsistency)
      queryLog.info("Streaming prepared query {} with params {}", statement.rawQuery, values)
      val rate = new Rate
      queryPipe(bound).andThen {
        case _ => queryLog.info(" Finished streaming prepared query {} with params {} {}", statement.rawQuery, values, rate)
      }
    }

    def prepare(query: String): Future[PreparedStatementDef] = {
      getStatement(query) {
        log.debug("Session preparing statement.")
        pipeline(Prepare(query, Some(keyspace))).map {
          case Responses.Prepared(_, statementDef) => statementDef

          case unknown =>
            throw new AthenaException(s"Unknown response to prepare request - $unknown")
        }
      }
    }

    import collection.JavaConversions._
    private[this] val statementCache: collection.concurrent.Map[String, Future[PreparedStatementDef]] =
      new java.util.concurrent.ConcurrentHashMap[String, Future[PreparedStatementDef]]()

    private def getStatement(key: String)(genValue: => Future[PreparedStatementDef]): Future[PreparedStatementDef] = {
      val promise = Promise[PreparedStatementDef]()
      statementCache.putIfAbsent(key, promise.future) match {
        case None =>
          val future = genValue
          future.onComplete { value =>
            promise.complete(value)
            // in case of exceptions we remove the cache entry (i.e. try again later)
            if (value.isFailure) statementCache.remove(key, promise)
          }
          future
        case Some(existingFuture) => existingFuture
      }
    }
  }

}

