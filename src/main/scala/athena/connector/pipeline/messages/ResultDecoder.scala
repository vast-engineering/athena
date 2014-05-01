package athena.connector.pipeline.messages

import athena._
import athena.connector._
import athena.data.{ColumnDef, DataType, Metadata}
import athena.util.{MD5Hash, ByteStringUtils}
import CassandraResponses._

import akka.util.{ByteString, ByteIterator}
import java.nio.ByteOrder

import scala.annotation.tailrec

/**
 * A set of objects that know how to parse the various sub-responses for the RESULT response opcode.
 */
private object ResultDecoder {

  private[this] val HasGlobalTablesSpecFlag = 1
  private[this] val HasMorePagesFlag = 2
  private[this] val HasNoMetadataFlag = 4

  def decodeSetKeyspace(it: ByteIterator)(implicit byteOrder: ByteOrder): KeyspaceResult = {
    KeyspaceResult(ByteStringUtils.readString(it))
  }

  /**
   * Decode a rows result. A rows result consists of the following, in order
   *
   * metadata - rows_count - rows
   *
   * Where metadata is decoded as below, rows_count is an int, and rows is
   * (rows_count * metadata.columnsCount) [byte] entries.
   *
   * For more details, see section 4.2.5.2 at
   * https://raw.github.com/apache/cassandra/trunk/doc/native_protocol_v2.spec
   *
   */
  def decodeRows(it: ByteIterator)(implicit byteOrder: ByteOrder): RowsResult = {
    val metadata = decodeMetaData(it)
    val rowCount = it.getInt
    val columnCount = metadata.columnsCount

    val resultBuilder = Seq.newBuilder[IndexedSeq[ByteString]]
    var rowIdx = 0
    while(rowIdx < rowCount) {
      var colIdx = 0
      val rowBuilder = IndexedSeq.newBuilder[ByteString]
      while(colIdx < columnCount) {
        rowBuilder += ByteStringUtils.readBytes(it)
        colIdx = colIdx + 1
      }
      resultBuilder += rowBuilder.result()
      rowIdx = rowIdx + 1
    }
    RowsResult(metadata, resultBuilder.result())
  }

  def decodeMetaData(it: ByteIterator)(implicit byteOrder: ByteOrder): Metadata = {

    val flags = it.getInt
    val columnsCount = it.getInt

    val pagingState = if ((flags & HasMorePagesFlag) != 0) {
      Some(ByteStringUtils.readBytes(it))
    } else {
      None
    }

    val columnDefs = if ((flags & HasNoMetadataFlag) != 0) {
      None
    } else {
      val (defaultKeyspaceName, defaultTableName) = if ((flags & HasGlobalTablesSpecFlag) != 0) {
        val ksn = ByteStringUtils.readString(it)
        val tn = ByteStringUtils.readString(it)
        (Some(ksn), Some(tn))
      } else {
        (None, None)
      }

      val columnDefs = IndexedSeq.newBuilder[ColumnDef]
      var index = 0
      while (index < columnsCount) {
        val keyspaceName = defaultKeyspaceName.getOrElse(ByteStringUtils.readString(it))
        val tableName = defaultTableName.getOrElse(ByteStringUtils.readString(it))
        val name = ByteStringUtils.readString(it)
        val dataType = DataType.fromByteIterator(it)
        columnDefs += ColumnDef(keyspaceName, tableName, name, dataType)
        index = index + 1
      }

      Some(columnDefs.result())
    }

    Metadata(columnsCount, columnDefs, pagingState)
  }

  def decodePepared(it: ByteIterator)(implicit bo: ByteOrder): PreparedResult = {
    val id = ByteStringUtils.readShortBytes(it)
    val meta = decodeMetaData(it)
    val resultMetadata = decodeMetaData(it)
    PreparedResult(MD5Hash(id), meta, resultMetadata)
  }

  def decodeSchemaChange(it: ByteIterator)(implicit bo: ByteOrder): SchemaChange = {
    SchemaChange(ByteStringUtils.readString(it), ByteStringUtils.readString(it), ByteStringUtils.readString(it))
  }


}
