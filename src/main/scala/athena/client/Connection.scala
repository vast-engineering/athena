package athena.client

import akka.actor.{ActorSystem, ActorRef}
import akka.pattern._

import athena.data.CValue
import athena.Responses._
import athena.Athena.{InternalException, QueryTimeoutException, AthenaException}
import akka.event.Logging
import athena.Responses.Timedout
import athena.Responses.RequestFailed
import athena.Responses.ErrorResponse
import athena.Requests.{Statement, FetchRows, SimpleStatement}
import akka.util.{ByteString, Timeout}
import athena.{AthenaResponse, AthenaRequest}
import scala.concurrent.Future

/**
 * A more user-friendly API to assist with executing queries against a connection to Cassandra.
 *
 * This class is able to dispatch requests at any level - connection, node, or cluster. The ActorRef given to
 * the constructor defines where the requests are sent.
 *
 */
class Connection(connectionActor: ActorRef)(implicit protected[client] val system: ActorSystem, timeout: Timeout) {

  import system.dispatcher

  private[this] val log = Logging.getLogger(system, this)

  def execute(query: String, values: Seq[CValue] = Seq()): Future[ResultPage] = {
    val stmt = SimpleStatement(query, values)
    sendRequest(stmt).map {
      case resp: Rows =>
        new ResultPage(resp, this)
      case x =>
        log.error("Expected a Rows result for a query, but got {} - query: {}", x, stmt)
        throw new InternalException(s"Unknown response to query $stmt - $x")
    }
  }

  protected[client] def loadPage(stmt: Statement, ps: ByteString): Future[Rows] = {
    val req = FetchRows(stmt, ps)
    sendRequest(req).map {
      case resp: Rows =>
        resp
      case x =>
        log.error("Expected a Rows result for a FetchRows, but got {} - query: {}", x, req)
        throw new InternalException(s"Unknown response to query $req - $x")
    }
  }

  private def sendRequest(req: AthenaRequest): Future[AthenaResponse] = {
    connectionActor.ask(req).map {
      case t: Timedout =>
        throw new QueryTimeoutException("Query execution timed out.")
      case e: RequestFailed =>
        throw new AthenaException(s"Request failed for unknown reason - $e")
      case ErrorResponse(_, error) =>
        log.error("Received error response for query {} - {}", req, error)
        throw error.toThrowable
      case resp: AthenaResponse =>
        resp
      case x =>
        log.error("Unknown response to query {} - {}", req, x)
        throw new InternalException(s"Unknown response to query $req - $x")
    }
  }

}
