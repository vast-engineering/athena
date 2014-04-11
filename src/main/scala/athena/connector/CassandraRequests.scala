package athena.connector

import akka.util.ByteString
import athena.data._
import athena.{ClusterEventName, SerialConsistency, Consistency}
import Consistency._
import SerialConsistency._

/**
 * A base trait for all Cassandra requests - these map 1:1 to opcodes defined here -
 *
 * https://raw.github.com/apache/cassandra/trunk/doc/native_protocol_v2.spec
 *
 * These are low-level protocol specific requests - these are not intended to be used by connector code.
 */
private[connector] object CassandraRequests {

  //internal use only!

  sealed trait CassandraRequest

  case object Startup extends CassandraRequest

  case class QueryParams(consistency: Consistency,
                         serialConsistency: SerialConsistency,
                         resultPageSize: Option[Int],
                         params: Seq[CValue] = IndexedSeq(),
                         pagingState: Option[ByteString] = None)

  case class QueryRequest(query: String, params: QueryParams) extends CassandraRequest
  case class ExecuteRequest(statementId: ByteString, params: QueryParams, excludeMetadata: Boolean) extends CassandraRequest

  case class Register(eventNames: Seq[ClusterEventName]) extends CassandraRequest

  case class Prepare(query: String) extends CassandraRequest

}


