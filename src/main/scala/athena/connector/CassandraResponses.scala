package athena.connector

import athena.Athena._
import athena.data.Metadata

import akka.util.ByteString
import athena.connector.CassandraResponses.CassandraResponse

/**
 * A base trait for all Cassandra responses. These map 1:1 to the responses defined here -
 *
 * https://raw.github.com/apache/cassandra/trunk/doc/native_protocol_v2.spec
 *
 * These are low-level protocol specific responses - these are not intended to be used by connector code.
 */
private[connector] object CassandraResponses {

  sealed trait CassandraResponse

  case object Ready extends CassandraResponse

  sealed abstract class Result extends CassandraResponse

  case object SuccessfulResult extends Result
  case class RowsResult(metadata: Metadata, data: Seq[IndexedSeq[ByteString]]) extends Result
  case class KeyspaceResult(keyspace: String) extends Result
}

//Error objects will be directly emitted by the various actors
trait CassandraError extends CassandraResponse with Error
