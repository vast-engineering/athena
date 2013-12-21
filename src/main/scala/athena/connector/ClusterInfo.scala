package athena.connector

import java.net.InetAddress
import athena.data.DataType
import athena.client.Row

private[connector] object ClusterInfo {
  case class ClusterMetadata(name: Option[String],
                             partitioner: Option[String],
                             hosts: Map[InetAddress, Host])

  case class Host(address: InetAddress, datacenter: Option[String] = None, rack: Option[String] = None)

  case class IndexMetadata(name: String, customClassName: Option[String] = None)

  case class ColumnMetadata(name: String, dataType: DataType, index: Option[IndexMetadata])

  case class TableMetadata(name: String, partitionKey: Seq[ColumnMetadata], clusteringColumns: Seq[ColumnMetadata],
                                              columns: Seq[ColumnMetadata])
  object TableMetadata {

    private val CF_NAME = "columnfamily_name"
    private val KEY_VALIDATOR = "key_validator"
    private val COMPARATOR = "comparator"

    def fromRow(row: Row, columnRows: Seq[Row]): TableMetadata = {
      val name = row.value(CF_NAME).as[String]

      //TODO: Implement this crap
      TableMetadata(name, Seq(), Seq(), Seq())
    }
  }



  case class KeyspaceMetadata(name: String,
                              durableWrites: Boolean,
                              tables: Seq[TableMetadata],
                              replicationStrategy: Option[ReplicationStrategy])


}
