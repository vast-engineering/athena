package athena

import athena.data.{PreparedStatementDef, CValue}
import akka.util.ByteString
import Consistency.Consistency
import SerialConsistency.SerialConsistency

/**
 * A command that will ultimately result in a network request
 */

object Requests {

  sealed trait AthenaRequest

  sealed trait KeyspaceAwareRequest extends AthenaRequest {
    def keyspace: Option[String]
  }


  /**
   * A Command that instructs a connection, node or cluster to execute a query
   */
  sealed trait QueryCommand extends KeyspaceAwareRequest

  /**
   * A command to fetch a new page of rows from an existing query.
   */
  case class FetchRows(statement: Statement, pagingState: ByteString) extends QueryCommand {
    override val keyspace: Option[String] = statement.keyspace
  }

  sealed trait Statement extends QueryCommand {
    def keyspace: Option[String]

    def values: Seq[CValue]

    def consistency: Option[Consistency]

    def serialConsistency: Option[SerialConsistency]

    def fetchSize: Option[Int]
  }

  /**
   * A command to execute a basic CQL query. If the consistency, serialConsistency or fetchSize options are not
   * specified, the connection's defaults will be used.
   */
  case class SimpleStatement(query: String,
                             values: Seq[CValue] = Seq.empty,
                             keyspace: Option[String] = None,
                             consistency: Option[Consistency] = None,
                             serialConsistency: Option[SerialConsistency] = None,
                             fetchSize: Option[Int] = None) extends Statement


  //used for prepared statements

  /**
   * Execute a prepared statement.
   */
  case class BoundStatement(statementDef: PreparedStatementDef,
                            values: Seq[CValue] = Seq.empty,
                            routingKey: Option[ByteString] = None,
                            consistency: Option[Consistency] = None,
                            serialConsistency: Option[SerialConsistency] = None,
                            fetchSize: Option[Int] = None) extends Statement {
    override val keyspace: Option[String] = statementDef.keyspace
  }

  /**
   * A command that will create a prepared statement.
   */
  case class Prepare(query: String, keyspace: Option[String]) extends KeyspaceAwareRequest


}
