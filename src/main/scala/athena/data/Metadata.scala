package athena.data

import akka.util.ByteString
import athena.ColumnDef

/**
 * Used to describe result columns in a query, and input parameters for a prepared statement.
 */
case class Metadata(columnsCount: Int, columns: Option[IndexedSeq[ColumnDef]], pagingState: Option[ByteString])

