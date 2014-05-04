package athena.connector

import athena.Athena._
import athena.data.Metadata

import akka.util.ByteString
import athena.connector.CassandraResponses.CassandraResponse
import java.net.InetSocketAddress
import athena.util.MD5Hash

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

  sealed trait Result extends CassandraResponse

  case object SuccessfulResult extends Result
  case class RowsResult(metadata: Metadata, data: Seq[IndexedSeq[ByteString]]) extends Result
  case class KeyspaceResult(keyspace: String) extends Result
  case class PreparedResult(id: MD5Hash, metadata: Metadata, resultMetadata: Metadata) extends Result
  case class SchemaChange(change: String, keyspace: String, table: String) extends Result

  sealed trait ClusterEvent extends CassandraResponse

  sealed abstract class TopologyEvent extends ClusterEvent {
    def host: InetSocketAddress
  }
  case class NewNode(host: InetSocketAddress) extends TopologyEvent
  case class NodeRemoved(host: InetSocketAddress) extends TopologyEvent
  case class NodeMoved(host: InetSocketAddress) extends TopologyEvent
  object TopologyEvent {
    def apply(eventName: String, addr: InetSocketAddress): TopologyEvent = {
      eventName match {
        case "NEW_NODE" => NewNode(addr)
        case "REMOVED_NODE" => NodeRemoved(addr)
        case "MOVED_NODE" => NodeMoved(addr)
        case x => throw new InternalException(s"Unknown event type $x")
      }
    }

  }

  case class HostStatusEvent(host: InetSocketAddress, isUp: Boolean) extends ClusterEvent
  object HostStatusEvent {
    def apply(eventName: String, addr: InetSocketAddress): HostStatusEvent = {
      eventName match {
        case "UP" => HostStatusEvent(addr, true)
        case "DOWN" => HostStatusEvent(addr, false)
        case x => throw new InternalException(s"Unknown event type $x")
      }
    }
  }

  sealed abstract class SchemaEvent extends ClusterEvent {
    def keyspace: String
    def table: String
  }
  object SchemaEvent {
    def apply(eventName: String, keyspace: String, table: String): SchemaEvent = {
      eventName match {
        case "CREATED" => SchemaCreated(keyspace, table)
        case "UPDATED" => SchemaUpdated(keyspace, table)
        case "DROPPED" => SchemaDropped(keyspace, table)
        case x => throw new InternalException(s"Unknown event type $x")
      }
    }
  }
  case class SchemaCreated(keyspace: String, table: String) extends SchemaEvent
  case class SchemaUpdated(keyspace: String, table: String) extends SchemaEvent
  case class SchemaDropped(keyspace: String, table: String) extends SchemaEvent

}

//Error objects will be directly emitted by the various actors
trait CassandraError extends CassandraResponse with Error
