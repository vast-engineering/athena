package com.vast.verona.client.pipeline

import com.vast.verona._
import akka.util.{ByteString, ByteIterator}
import java.nio.ByteOrder
import scala.Some
import scala.collection.mutable
import com.vast.verona.data.{ColumnDef, Metadata, DataType}
import scala.annotation.tailrec
import com.vast.verona.client.{Successful, Result, Rows}
import com.typesafe.scalalogging.slf4j.Logging

/**
 * A set of objects that know how to parse the various sub-responses for the RESULT response opcode.
 */
private[pipeline] object ResultDecoder extends Logging {

  private[this] val HasGlobalTablesSpecFlag = 1
  private[this] val HasMorePagesFlag = 2
  private[this] val HasNoMetadataFlag = 4

  def decodeMetaData(it: ByteIterator)(implicit byteOrder: ByteOrder): Metadata = {

    val flags = it.getInt
    val columnsCount = it.getInt

    val pagingState = if ((flags & HasMorePagesFlag) != 0) {
      val size = it.getInt
      Some(ByteString(it.take(size).toArray))
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

      //mutable collection, so sue me
      val columnDefs = new mutable.ArrayBuffer[ColumnDef](columnsCount)
      var index = 0
      while (index < columnsCount) {
        val keyspaceName = defaultKeyspaceName.getOrElse(ByteStringUtils.readString(it))
        val tableName = defaultTableName.getOrElse(ByteStringUtils.readString(it))
        val name = ByteStringUtils.readString(it)
        val dataType = DataType.fromByteIterator(it)
        columnDefs.append(ColumnDef(keyspaceName, tableName, name, dataType))
        index = index + 1
      }

      Some(columnDefs)
    }

    Metadata(columnsCount, columnDefs, pagingState)
  }

  def decodeRows(it: ByteIterator)(implicit byteOrder: ByteOrder): Rows = {
    val metadata = decodeMetaData(it)
    val rowCount = it.getInt

    val remaining = it.toByteString

    //split the front of the ByteString into a number of chunks, returning the remainder
    @tailrec def splitByteString(chunks: Int, input: ByteString, acc: List[ByteString]): (List[ByteString], ByteString) = {
      if (chunks == 0) {
        (acc.reverse, input)
      } else {
        val it = input.iterator
        val size = it.getInt
        val (data, remainder) = it.toByteString.splitAt(size)
        splitByteString(chunks - 1, remainder, data :: acc)
      }
    }

    @tailrec def splitRows(rows: Int, input: ByteString, acc: List[List[ByteString]]): List[List[ByteString]] = {
      if (rows == 0) {
        if (!input.isEmpty) {
          //uh-oh, should have exhausted the input
          logger.error("Likely bug in Rows result parser - input not exhausted.")
          throw new InternalException("Rows input not exhausted.")
        }
        acc.reverse
      } else {
        val (row, remainder) = splitByteString(metadata.columnsCount, input, Nil)
        splitRows(rows - 1, remainder, row :: acc)
      }
    }

    val rowData = splitRows(rowCount, remaining, Nil)
    Rows(metadata, rowData)
  }
}
