package athena.connector

import scala.concurrent.{ExecutionContext, Future}
import athena.Requests.{SimpleStatement, Statement}
import athena.client.Row
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import java.net.{InetSocketAddress, InetAddress}
import akka.actor.ActorLogging
import athena.Athena.AthenaException
import play.api.libs.json.Json
import akka.event.LoggingAdapter


private[connector] trait ClusterUtils {

  import ClusterUtils._

  import ClusterInfo._

  def log: LoggingAdapter

  def updateClusterInfo(connectedHost: InetAddress, pipeline: Statement => Enumerator[Row])(implicit ec: ExecutionContext): Future[ClusterMetadata] = {

    //An iteratee that goes through and updates the host entries in the hosts map for all the peer hosts
    //note - this does *not* update the host we're connected to - that gets taken care of below
    val updatedPeersInfoF: Future[Map[InetAddress, Host]] = pipeline(SimpleStatement(PEERS_QUERY)).run(Iteratee.fold[Row, Map[InetAddress, Host]](Map()) { (hosts, row) =>
      val address = row.value("rpc_address").as[Option[InetAddress]].collect {
        case addr if addr == BindAllAddress =>
          //get the alternate peer address instead
          val peer = row.value("peer").as[InetAddress]
          log.warning("Host {} has 0.0.0.0 as rpc_address, using listen_address ({}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", peer, peer)
          peer
        case addr => addr
      } getOrElse {
        val peer = row.value("peer").as[InetAddress]
        log.error("No rpc_address found for host {} in {}'s peers system table. That should not happen but using address {} instead", peer, connectedHost, peer)
        peer
      }
      val datacenter = row.value("data_center").as[Option[String]]
      val rack = row.value("rack").as[Option[String]]
      hosts.updated(address, Host(address, datacenter, rack))
    })

    val localInfoF: Future[LocalClusterInfo] = pipeline(SimpleStatement(LOCAL_QUERY)).run(Iteratee.head[Row].map { rowOpt =>
      val row = rowOpt.getOrElse {
        throw new AthenaException("Could not determine host info for the current connected node.")
      }
      val clusterName = row.value("cluster_name").as[Option[String]]
      val paritioner = row.value("partitioner").as[Option[String]]
      val datacenter = row.value("data_center").as[Option[String]]
      val rack = row.value("rack").as[Option[String]]
      val tokens = row.value("tokens").as[Set[String]]
      LocalClusterInfo(clusterName, paritioner, datacenter, rack, tokens)
    })

    for {
      localInfo <- localInfoF
      peersInfo <- updatedPeersInfoF
    } yield {
      //update the local connected host entry
      val localHost = Host(connectedHost, localInfo.datacenter, localInfo.rack)
      val allHosts = peersInfo.updated(connectedHost, localHost)
      ClusterMetadata(name = localInfo.clusterName, partitioner = localInfo.partitioner, hosts = allHosts)
    }
  }

  def allKeyspaceMetadata(pipeline: Statement => Enumerator[Row])(implicit ec: ExecutionContext): Future[Map[String, KeyspaceMetadata]] = {

    //mapping from keyspace to column family
    val columnFamilyDefF: Future[Map[String, List[Row]]] = pipeline(SimpleStatement(SELECT_COLUMN_FAMILIES)).run(Iteratee.getChunks[Row].map { rows =>
      rows.groupBy(_.value(KS_NAME).as[String])
    })

    //mapping of keyspace -> (columnFamily -> column)
    val columnDefsF: Future[Map[String, Map[String, List[Row]]]] = pipeline(SimpleStatement(SELECT_COLUMNS)).run(Iteratee.getChunks[Row].map { rows =>
      val ksMap: Map[String, List[Row]] = rows.groupBy(_.value(KS_NAME).as[String])
      ksMap.mapValues { ksRows =>
        ksRows.groupBy(_.value(CF_NAME).as[String])
      }
    })

    def keyspaces(columnFamilies: Map[String, List[Row]], columnDefs: Map[String, Map[String, List[Row]]]) = {
      val it = Iteratee.fold[Row, Map[String, KeyspaceMetadata]](Map[String, KeyspaceMetadata]()) { (keyspaces, row) =>
        val ksName = row.value(KS_NAME).as[String]
        val durableWrites = row.value(DURABLE_WRITES).as[Boolean]
        val replicationStrategy = row.value(STRATEGY_CLASS).as[Option[String]].flatMap { strategyClass =>
          val replOptions = Json.parse(row.value(STRATEGY_OPTIONS).as[String]).as[Map[String, String]]
          ReplicationStrategy(strategyClass, replOptions)
        }
        val tables = for {
          columnFamilyDefs <- columnFamilies.get(ksName)
          columnsByFamily <- columnDefs.get(ksName)
        } yield {
          columnFamilyDefs.map { cfRow =>
            val cfName = cfRow.value(CF_NAME).as[String]
            TableMetadata.fromRow(cfRow, columnsByFamily.get(cfName).getOrElse(Seq()))
          }
        }
        keyspaces.updated(ksName, KeyspaceMetadata(ksName, durableWrites, tables.getOrElse(Seq()), replicationStrategy))
      }
      pipeline(SimpleStatement(SELECT_KEYSPACES_QUERY)).run(it)
    }

    for {
      columnFamilies <- columnFamilyDefF
      columnDefs <- columnDefsF
      keyspaces <- keyspaces(columnFamilies, columnDefs)
    } yield {
      keyspaces
    }

  }

}

object ClusterUtils {
  private val PEERS_QUERY = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers"
  private val LOCAL_QUERY = "SELECT cluster_name, partitioner, data_center, rack, tokens  FROM system.local WHERE key='local'"

  private val SELECT_KEYSPACES_QUERY = "SELECT * FROM system.schema_keyspaces"
  private val SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies"
  private val SELECT_COLUMNS = "SELECT * FROM system.schema_columns"


  //some column names
  private val KS_NAME = "keyspace_name"
  private val DURABLE_WRITES = "durable_writes"
  private val STRATEGY_CLASS = "strategy_class"
  private val STRATEGY_OPTIONS = "strategy_options"
  private val CF_NAME = "columnfamily_name"

  //holds the info about the node that we're connected to
  private case class LocalClusterInfo(clusterName: Option[String], partitioner: Option[String],
                                            datacenter: Option[String], rack: Option[String], tokens: Set[String])

  //the default bind all address - 0.0.0.0
  private val BindAllAddress = InetAddress.getByAddress(new Array[Byte](4))
}