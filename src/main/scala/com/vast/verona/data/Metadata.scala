package com.vast.verona.data

import akka.util.ByteString

/**
 * Used to describe result columns in a query, and input parameters for a prepared statement.
 */
case class Metadata(columnsCount: Int, columns: Option[IndexedSeq[ColumnDef]], pagingState: Option[ByteString])

case class ColumnDef(keyspace: String, table: String, name: String, dataType: DataType)

