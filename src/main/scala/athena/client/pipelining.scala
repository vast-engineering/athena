package athena.client

import athena.Requests.{FetchRows, Statement, AthenaRequest}
import scala.concurrent.{ExecutionContext, Future}
import athena.Responses._
import akka.actor.ActorRef
import akka.util.{ByteString, Timeout}
import athena.Athena.{NoHostAvailableException, InternalException, AthenaException, QueryTimeoutException}
import akka.pattern._
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import athena.Responses.Timedout
import athena.Responses.ErrorResponse
import spray.util.LoggingContext
import scala.collection.mutable

object pipelining {
  type Pipeline = AthenaRequest => Future[AthenaResponse]

  type QueryPipeline = Statement => Enumerator[Row]
  type UpdatePipeline = Statement => Future[Seq[Row]]

  def pipeline(connection: Future[ActorRef])
              (implicit log: LoggingContext, ec: ExecutionContext, timeout: Timeout): AthenaRequest => Future[AthenaResponse] = {
    val pipeF = connection.map(pipeline)
    request => pipeF.flatMap(pipe => pipe(request))
  }

  def pipeline(connection: ActorRef)(implicit log: LoggingContext, ec: ExecutionContext, timeout: Timeout): AthenaRequest => Future[AthenaResponse] = {
    request =>
      connection.ask(request).map {
        case ConnectionUnavailable(_, errors) =>
          throw new NoHostAvailableException("No hosts available for request.", errors)

        case t: Timedout =>
          throw new QueryTimeoutException("Query execution timed out.")

        case resp: AthenaResponse if resp.isFailure =>
          throw new AthenaException(s"Request failed - $resp")

        case ErrorResponse(_, error) =>
          log.error("Error with request {}, error={}", request, error)
          throw error.toThrowable
          
        case resp: AthenaResponse =>
          resp

        case x =>
          log.error("Unknown response to query {} - {}", request, x)
          throw new InternalException(s"Unknown response to query $request - $x")
      } recover {
        case e: AskTimeoutException =>
          throw new QueryTimeoutException("Query execution timed out.")
      }
  }

  def queryPipeline(pipeline: Pipeline)(implicit ec: ExecutionContext, timeout: Timeout, log: LoggingContext): Statement => Future[Seq[Row]] = {
    stmt => {
      def collectRows(acc: mutable.Builder[Row, Seq[Row]], meta: Option[ResultSetMetadata], ps: Option[ByteString]): Future[Seq[Row]] = {
        getRows(pipeline, stmt, ps).flatMap { rowsOpt =>
          rowsOpt.map { rows =>
            val metadata = meta.getOrElse(ResultSetMetadata(rows))
            rows.data.foreach { rowData =>
              acc += Row(metadata, rowData)
            }
            if(rows.pagingState.isEmpty) {
              //this means we've exhausted the results
              Future.successful(acc.result())
            } else {
              //we need to fetch more rows
              collectRows(acc, Some(metadata), rows.pagingState)
            }
          } getOrElse {
            //this means the query returned no rows - we can just return the current rows
            Future.successful(acc.result())
          }
        }
      }

      collectRows(Seq.newBuilder, None, None)
    }
  }

  def updatePipeline(connection: ActorRef)(implicit ec: ExecutionContext, timeout: Timeout, log: LoggingContext): Statement => Future[Seq[Row]] = {
    queryPipeline(pipeline(connection))
  }

  /**
   * Create a new pipeline that has the ability to asynchronously execute a Statement and
   * return an Enumerator of the resulting rows.
   */
  def streamingPipeline(pipeline: Pipeline)(implicit ec: ExecutionContext, timeout: Timeout, log: LoggingContext): Statement => Enumerator[Row] = {
    val rowsEnum = rowsEnumerator(pipeline)

    stmt => rowsEnum(stmt)
  }

  def queryPipeline(connection: ActorRef)(implicit ec: ExecutionContext, timeout: Timeout, log: LoggingContext): Statement => Enumerator[Row] = {
    streamingPipeline(pipeline(connection))
  }

  private case class EnumeratorState(pageInfo: Option[ByteString] = None, metadata: Option[ResultSetMetadata] = None, beforeFirstPage: Boolean = true)
  private case class ResultPage(metadata: ResultSetMetadata, data: Seq[IndexedSeq[ByteString]])

  private def rowsEnumerator(pipeline: Pipeline)(implicit ec: ExecutionContext, log: LoggingContext): Statement => Enumerator[Row] = {
    stmt =>

      Enumerator.unfoldM[EnumeratorState, ResultPage](EnumeratorState()) { state =>
        if(!state.beforeFirstPage && state.pageInfo.isEmpty) {
          //this means that we've fetched pages and there are no more pages to fetch
          Future.successful(None)
        } else {
          getRows(pipeline, stmt, state.pageInfo).map { rowsOpt =>
            //optimization - if there are no rows in the page, then don't return anything
            rowsOpt.filter(!_.data.isEmpty).map { rows =>
            //only create the metadata once - it will be the same for every page
              val pageMetadata = state.metadata.getOrElse(ResultSetMetadata(rows))
              val nextState = EnumeratorState(rows.pagingState, Some(pageMetadata), beforeFirstPage = false)
              val resultPage = ResultPage(pageMetadata, rows.data)
              (nextState, resultPage)
            }
          }
        }
      } through {
        //this bit transforms from an Enumerator[ResultPage] to an Enumerator[Row]
        Enumeratee.mapConcat[ResultPage] { page =>
          val meta = page.metadata
          page.data.map(data => Row(meta, data))
        }
      } andThen Enumerator.eof
  }

  private def getRows(pipeline: Pipeline, stmt: Statement, ps: Option[ByteString])(implicit ec: ExecutionContext, log: LoggingContext): Future[Option[Rows]] = {
    val query = ps.map(FetchRows(stmt, _)).getOrElse(stmt)
    pipeline(query).map {
      case rows: Rows => Some(rows)
      case s: Successful =>
        //The query had no rows - just end the enumerator
        None
      case x =>
        throw new InternalException(s"Expected Rows back from a query, got $x instead.")
    }
  }


}
