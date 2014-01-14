package athena.client

import athena.Requests.{FetchRows, Query, Statement, AthenaRequest}
import scala.concurrent.{ExecutionContext, Future}
import athena.Responses._
import akka.actor.ActorRef
import akka.util.Timeout
import athena.Athena.{InternalException, AthenaException, QueryTimeoutException}
import akka.pattern._
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import athena.Responses.Timedout
import athena.Responses.ErrorResponse
import spray.util.LoggingContext
import akka.dispatch.ForkJoinExecutorConfigurator.AkkaForkJoinTask

object Pipelining {
  type Pipeline = AthenaRequest => Future[AthenaResponse]

  def pipeline(connection: Future[ActorRef])(implicit log: LoggingContext, ec: ExecutionContext, timeout: Timeout): AthenaRequest => Future[AthenaResponse] = {
    val pipeF = connection.map(pipeline)
    request => pipeF.flatMap(pipe => pipe(request))
  }

  def pipeline(connection: ActorRef)(implicit log: LoggingContext, ec: ExecutionContext, timeout: Timeout): AthenaRequest => Future[AthenaResponse] = {
    request =>
      connection.ask(request).map {
        case t: Timedout =>
          throw new QueryTimeoutException("Query execution timed out.")
        case ErrorResponse(_, error) =>
          throw error.toThrowable
        case resp: AthenaResponse =>
          if(resp.isFailure) {
            throw new AthenaException(s"Request failed for unknown reason - $resp")
          } else {
            resp
          }
        case x =>
          log.error("Unknown response to query {} - {}", request, x)
          throw new InternalException(s"Unknown response to query $request - $x")
      } recover {
        case e: AskTimeoutException =>
          throw new QueryTimeoutException("Query execution timed out.")
      }
  }

  def updatePipeline(pipeline: Pipeline)(implicit ec: ExecutionContext, timeout: Timeout, log: LoggingContext): Statement => Future[Unit] = {
    stmt =>
      pipeline(stmt).map {
        case s: Successful =>
          //everything went as planned
          ()
        case r: Rows =>
          log.warning("Got rows back from update query.")
          //just ignore them
          ()
        case x =>
          throw new InternalException(s"Expected Successful back from an update, got $x instead.")
      }
  }

  /**
   * Create a new pipeline that has the ability to asynchronously execute a Statement and
   * return an Enumerator of the resulting rows.
   */
  def queryPipeline(pipeline: Pipeline)(implicit ec: ExecutionContext, timeout: Timeout): Statement => Enumerator[Row] = {
    val rowsEnum = rowsEnumerator(pipeline)

    stmt => rowsEnum(stmt)
  }

  def queryPipeline(connection: ActorRef)(implicit ec: ExecutionContext, timeout: Timeout): Statement => Enumerator[Row] = {
    queryPipeline(pipeline(connection))
  }

  private def rowEnumeratee(implicit ec: ExecutionContext): Enumeratee[Rows, Row] = Enumeratee.mapConcat { rows =>
    rows.data.map { rowData =>
      Row(rows.columnDefs, rowData)
    }
  }

  private def rowsEnumerator(pipeline: Pipeline)(implicit ec: ExecutionContext, log: LoggingContext): Statement => Enumerator[Row] = {
    stmt =>
      val enum = Enumerator.unfoldM[Option[Query], Rows](Some(stmt)) { q =>
        if(q.isEmpty) {
          Future.successful(None)
        } else {
          pipeline(q.get).map {
            case rows@Rows(_, _, _, pagingState) =>
              val nextQ = pagingState.map(ps => FetchRows(stmt, ps))
              Some(nextQ, rows)
            case s: Successful =>
              //shouldn't really get this, but it's not technically an error
              log.warning("Expected rows from query, got empty Successful response instead.")
              None
            case x =>
              throw new InternalException(s"Expected Rows back from a query, got $x instead.")
          }
        }
      }
      enum.through(rowEnumeratee)
  }

}
