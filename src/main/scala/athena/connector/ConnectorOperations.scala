package athena.connector

import scala.concurrent.{ExecutionContext, Future}
import athena.Responses.{AthenaResponse, ErrorResponse, RequestFailed, Timedout}
import athena.Athena.{InternalException, AthenaException, QueryTimeoutException}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.pattern._
import akka.util.Timeout
import athena.Requests.AthenaRequest

object ConnectorOperations {

  def sendRequest(connectionActor: ActorRef, req: AthenaRequest)(implicit log: LoggingAdapter, ec: ExecutionContext, timeout: Timeout): Future[AthenaResponse] = {
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
        throw new InternalException(s"Unknown response to request $req - $x")
    }
  }


}
