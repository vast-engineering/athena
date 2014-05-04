package athena.data

import akka.util.ByteString
import athena.util.MD5Hash

/**
 * Used to describe the metadata around a single input parameter to a statement or a column in the output of a query
 */
case class ColumnDef(keyspace: String, table: String, name: String, dataType: DataType)

/**
 * Used to describe result columns in a rows response.
 */
case class Metadata(columnsCount: Int, columns: Option[IndexedSeq[ColumnDef]], pagingState: Option[ByteString])

case class PreparedStatementDef(id: MD5Hash,
                                rawQuery: String,
                                keyspace: Option[String],
                                parameterDefs: IndexedSeq[ColumnDef],
                                resultDefs: IndexedSeq[ColumnDef])
