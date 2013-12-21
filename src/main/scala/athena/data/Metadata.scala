package athena.data

import akka.util.ByteString

/**
 * Used to describe the metadata around a single input parameter to a statement or a column in the output of a query
 */
case class ColumnDef(keyspace: String, table: String, name: String, dataType: DataType)

/**
 * Used to describe result columns in a query, and input parameters for a prepared statement.
 */
case class Metadata(columnsCount: Int, columns: Option[IndexedSeq[ColumnDef]], pagingState: Option[ByteString])

