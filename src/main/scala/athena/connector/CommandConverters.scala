package athena.connector

import athena._
import akka.util.ByteString
import athena.data.{PreparedStatementDef, Metadata}
import spray.util.LoggingContext

private object CommandConverters {

  import CassandraRequests._
  import CassandraResponses._

  import Requests._
  import Responses._

  //a bunch of stuff to convert from Athena - this is lame and should probably be cleaned up.

  //TODO - not in love with these - should better modularize all of this
  def convertRequest(cmd: AthenaRequest, settings: QuerySettings)(implicit log: LoggingContext): CassandraRequest = cmd match {
    case stmt: Requests.Statement => convertStatement(stmt, settings, None)
    case Requests.FetchRows(stmt, pagingState) => convertStatement(stmt, settings, Some(pagingState))
    case Requests.Prepare(statement, _) => CassandraRequests.Prepare(statement)
  }

  def convertResponse(req: AthenaRequest, resp: CassandraResponse)(implicit log: LoggingContext): AthenaResponse = {
    (req, resp) match {
      case (x, err: CassandraError) =>
        ErrorResponse(x, err)

      case (q: QueryCommand, res: Result) =>
        resultToQueryEvent(q, res)

      case (p: Requests.Prepare, res: CassandraResponses.PreparedResult) =>
        if(res.metadata.columns.isEmpty || res.resultMetadata.columns.isEmpty) {
          throw new IllegalStateException("Unexpected response to prepare request - no column metadata.")
        }
        Prepared(p, PreparedStatementDef(res.id, p.query, p.keyspace, res.metadata.columns.get, res.resultMetadata.columns.get))

      case (_, Ready) =>
        //shouldn't happen
        //throw an exception because the connection should be killed.
        throw new IllegalStateException("A Ready response should not be generated after initialization.")

      case (_, evt: ClusterEvent) =>
        //shouldn't happen either - cluster events don't get translated
        log.error("Cannot convert a ClusterEvent to a response. Killing connection.")
        throw new IllegalStateException("Cluster events cannot be converted.")

      case (x, res: Result) =>
        //shouldn't happen - kill the connection
        throw new IllegalStateException(s"Got a Result response to a non-query request. Req - $x : Resp - $res")
    }
  }

  private def convertStatement(stmt: Statement,
                               settings: QuerySettings,
                               pagingState: Option[ByteString] = None)(implicit log: LoggingContext) = stmt match {
    case stmt: SimpleStatement => QueryRequest(stmt.query, queryParams(stmt, settings, pagingState))
    case stmt: BoundStatement => ExecuteRequest(stmt.statementDef.id, queryParams(stmt, settings, pagingState), excludeMetadata = true)
  }

  private def queryParams(stmt: Statement, settings: QuerySettings, pagingState: Option[ByteString]): QueryParams = {
    def sizeToOption(size: Int): Option[Int] = {
      if(size <= 0 || size == Int.MaxValue)
        None
      else
        Some(size)
    }

    val realFetchSize =
      stmt.fetchSize
        .flatMap(sizeToOption)
        .orElse(sizeToOption(settings.defaultFetchSize))

    QueryParams(
      stmt.consistency.getOrElse(settings.defaultConsistencyLevel),
      stmt.serialConsistency.getOrElse(settings.defaultSerialConsistencyLevel),
      realFetchSize,
      stmt.values,
      pagingState
    )
  }

  private def resultToQueryEvent(request: QueryCommand, result: Result)(implicit log: LoggingContext): SuccessfulResponse = {

    (request, result) match {
      case (x, SuccessfulResult) =>
        //any successful result just maps to Successful
        Successful(x)

      case (x, SchemaChange(_, _, _)) =>
        Successful(x)

      case (stmt: SimpleStatement, RowsResult(Metadata(_, Some(columns), pagingState), data)) =>
        //should always have metadata in a response to a simple statement
        Rows(stmt, columns, data, pagingState)

      case (stmt: SimpleStatement, _) =>
        //we got a rows result for a simple statement with no metadata
        // this is bad - shouldn't ever happen
        //throw an exception to kill the connection
        throw new IllegalStateException("Invalid response for simple statement.")

      case (stmt: BoundStatement, RowsResult(Metadata(_, columnsOpt, pagingState), data)) =>
        Rows(stmt, columnsOpt.getOrElse(stmt.statementDef.resultDefs), data, pagingState)

      case (stmt: BoundStatement, _) =>
        throw new IllegalStateException("Invalid response for bound statement.")

      case (FetchRows(stmt, _), r: RowsResult) =>
        resultToQueryEvent(stmt, r)

      case (FetchRows(stmt, _), _) =>
        throw new IllegalStateException("Invalid response for fetch rows.")

      case (_, x: KeyspaceResult) =>
        //shouldn't ever happen - clients can't call this
        throw new IllegalStateException("A set keyspace response cannot be converted.")

    }

  }

}
