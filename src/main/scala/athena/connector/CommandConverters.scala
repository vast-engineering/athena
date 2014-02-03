package athena.connector

import athena._
import athena.Athena.QueryExecutionException

private object CommandConverters {

  import CassandraRequests._
  import CassandraResponses._

  import Requests._
  import Responses._

  //a bunch of stuff to convert from Athena - this is lame and should probably be cleaned up.

  //TODO - not in love with these - should better modularize all of this
  def convertRequest(cmd: AthenaRequest, settings: QuerySettings): CassandraRequest = cmd match {
    case c: Query => queryToRequest(c, settings)
    //as more subtypes of DataCommand appear, add them here.
  }

  def convertResponse(req: AthenaRequest, resp: CassandraResponse): AthenaResponse = {
    (req, resp) match {
      case (x, err: CassandraError) =>
        ErrorResponse(x, err)
      case (_, Ready) =>
        //shouldn't happen
        throw new IllegalStateException("A Ready response should not be generated after initialization.")
      case (_, evt: ClusterEvent) =>
        //shouldn't happen either - cluster events don't get translated
        throw new IllegalStateException("Cluster events cannot be converted.")
      case (q: Query, res: Result) =>
        resultToQueryEvent(q, res)
      case (x, res: Result) =>
        //shouldn't happen
        throw new IllegalStateException(s"Got a Result response to a non-query request. Req - $x : Resp - $res")
    }
  }

  private def queryToRequest(cmd: Query, settings: QuerySettings): FetchRequest = cmd match {
    case SimpleStatement(query, values, consistency, serialConsistency, fetchSize) =>
      //Prevent users from manually switching keyspaces - if we're using a cluster connection,
      // swapping a keyspace on a single connection can (and will) cause *very* strange
      // behavior with other connections to the same host and other node connections.

      //TODO: If we want to support 'USE' queries, the result of a successful set keyspace
      //response *must* be propagated immediately up the chain to every connection in a node's pool
      //and up to the cluster so that it updates all other node connection pools as well.
      if(query.trim.take(3).toUpperCase == "USE") {
        throw new QueryExecutionException("USE queries are not supported. If you need to use another keyspace, open up a new cluster connection to that keyspace.")
      }

      def sizeToOption(size: Int): Option[Int] = {
        if(size <= 0 || size == Int.MaxValue)
          None
        else
          Some(size)
      }

      val realFetchSize = fetchSize.flatMap(sizeToOption).orElse(sizeToOption(settings.defaultFetchSize))

      QueryRequest(query,
        consistency.getOrElse(settings.defaultConsistencyLevel),
        serialConsistency.getOrElse(settings.defaultSerialConsistencyLevel),
        realFetchSize,
        values, None)
    case BoundStatement(statementDef, params, keyspace, routingKey, consistency, serialConsistency, fetchSize) =>
      throw new Athena.InternalException("Bound prepared statements not supported yet.")
    case FetchRows(stmt, pagingState) =>
      queryToRequest(stmt, settings).withPagingState(Some(pagingState))
  }


  private def resultToQueryEvent(req: Query, result: Result): QueryResult = {

    (req, result) match {
      case (x, SuccessfulResult) =>
        //any successful result just maps to Successful
        Successful(x)

      case (stmt: SimpleStatement, RowsResult(meta, data)) =>
        //should always have metadata in a response to a simple statement
        Rows(stmt, meta.columns.get, data, meta.pagingState)

      case (stmt: BoundStatement, RowsResult(meta, data)) =>
        Rows(stmt, meta.columns.getOrElse(stmt.statementDef.parameterDefs), data, meta.pagingState)

      case (FetchRows(stmt, _), r: RowsResult) =>
        resultToQueryEvent(stmt, r)

      case (_, x: KeyspaceResult) =>
        //shouldn't ever happen - clients can't call this
        throw new IllegalStateException("A set keyspace response cannot be converted.")

    }

  }

}
