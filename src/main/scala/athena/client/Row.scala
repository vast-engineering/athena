package athena.client

import athena.data.{ColumnDef, CValue}
import akka.util.ByteString
import scala.collection.mutable
import athena.Responses.Rows

private[client] class ResultSetMetadata(val columnDefs: IndexedSeq[ColumnDef]) {

  //internally used to cache the columnName -> index lookup
  private[this] val indexMap: mutable.Map[String, Int] = mutable.Map()

  def indexOfColumn(name: String): Int = {
    indexMap.getOrElseUpdate(name, {
      val index = columnDefs.indexWhere(_.name.equalsIgnoreCase(name))
      if(index < 0) {
        throw new IllegalArgumentException(s"Unknown column $name")
      }
      index
    })
  }
}

object ResultSetMetadata {
  def apply(rows: Rows): ResultSetMetadata = {
    new ResultSetMetadata(rows.columnDefs)
  }
}

class Row private (metadata: ResultSetMetadata, data: IndexedSeq[ByteString]) {

  val columns = metadata.columnDefs

  def value(index: Int): CValue = {
    if(index < 0 || index >= data.size) {
      throw new IllegalArgumentException(s"Invalid column index $index")
    }
    CValue.parse(metadata.columnDefs(index).dataType, data(index))
  }
  def value(name: String): CValue = value(metadata.indexOfColumn(name))
  def values: Iterator[CValue] = metadata.columnDefs.zip(data).iterator.map {
    case (columnDef, columnData) => CValue.parse(columnDef.dataType, columnData)
  }

  //TODO - add a 'rawValue' method that returns a CValue that just wraps the bytestring and data type
  //this will allow people to access the raw unparsed bytes if desired

  override def toString: String = {
    val nameTuples = values.zip(metadata.columnDefs.iterator.map(_.name)).map(t => s"${t._2} = ${t._1}").mkString(", ")
    s"Row($nameTuples)"
  }
}

object Row {
  def apply(metadata: ResultSetMetadata, data: IndexedSeq[ByteString]) = {
    if(metadata.columnDefs.size != data.size) {
      throw new IllegalArgumentException("Must have a column def for each data element.")
    }
    new Row(metadata, data)
  }
}
