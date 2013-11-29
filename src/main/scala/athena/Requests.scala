package athena

import athena.data.Consistency.Consistency
import athena.data.SerialConsistency.SerialConsistency
import athena.data.{SerialConsistency, Consistency, DataType, CValue}
import athena.util.MD5Digest
import akka.util.ByteString
import athena.data.Consistency.Consistency
import athena.data.SerialConsistency.SerialConsistency

/**
 * Used to describe the metadata around a single input parameter to a statement or a column in the output of a query
 */
case class ColumnDef(keyspace: String, table: String, name: String, dataType: DataType)

/**
 * A command that will ultimately result in a network request
 */
sealed trait AthenaRequest

object Requests {

  /**
   * A Command that instructs a connection, node or cluster to execute a query
   */
  sealed trait Query extends AthenaRequest

  sealed trait Statement extends Query {
    def consistency: Option[Consistency]
    def serialConsistency: Option[SerialConsistency]
    def fetchSize: Option[Int]
  }

  /**
   * A command to fetch a new page of rows from an existing query.
   */
  case class FetchRows(statement: Statement, pagingState: ByteString) extends Query

  /**
   * A command to execute a basic CQL query. If the consistency, serialConsistency or fetchSize options are not
   * specified, the connection's defaults will be used.
   */
  case class SimpleStatement(query: String,
                             values: Seq[CValue] = Seq(),
                             consistency: Option[Consistency] = None,
                             serialConsistency: Option[SerialConsistency] = None,
                             fetchSize: Option[Int] = None) extends Statement


  //used for prepared statements
  /**
   * The definition of a single input parameter to a prepared statement
   */
  case class PreparedStatementDef(id: MD5Digest, rawQuery: String, parameterDefs: IndexedSeq[ColumnDef], resultDefs: IndexedSeq[ColumnDef])

  //used internally - should only be able to be constructed by binding a PreparedStatementDef to a set of parameters
  private[athena] case class BoundStatement(statementDef: PreparedStatementDef, parameters: Seq[CValue],
                                            consistency: Option[Consistency], serialConsistency: Option[SerialConsistency], fetchSize: Option[Int]) extends Statement

  //TODO add batch statements
}
