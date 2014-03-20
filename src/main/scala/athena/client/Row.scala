package athena.client

import athena.data.{ColumnDef, CValue}
import akka.util.ByteString
import scala.collection.mutable
import athena.Responses.Rows

private[client] class ResultSetMetadata(val columnDefs: IndexedSeq[ColumnDef]) {

  //internally used to cache the columnName -> index lookup
  private[this] val indexMap: mutable.Map[String, Int] = mutable.Map()

  def indexOfColumn(name: String): Option[Int] = {
    indexMap.get(name).orElse {
      val index = columnDefs.indexWhere(_.name.equalsIgnoreCase(name))
      if (index >= 0) {
        //this means we found the column
        indexMap.put(name, index)
        Some(index)
      } else {
        None
      }
    }
  }
}

object ResultSetMetadata {
  def apply(rows: Rows): ResultSetMetadata = {
    new ResultSetMetadata(rows.columnDefs)
  }
}

class Row private (metadata: ResultSetMetadata, data: IndexedSeq[ByteString]) {

  val columns = metadata.columnDefs

  val size = metadata.columnDefs.size

  def apply(idx: Int): Option[CValue] = {
    if(idx < 0 || idx > data.size) {
      None
    } else {
      Some(CValue.parse(metadata.columnDefs(idx).dataType, data(idx)))
    }
  }
  def apply(name: String): Option[CValue] = {
    metadata.indexOfColumn(name).flatMap(idx => apply(idx))
  }

  def value(index: Int): CValue = {
    apply(index).getOrElse(throw new IllegalArgumentException(s"Invalid column index $index"))
  }
  def value(name: String): CValue = {
    apply(name).getOrElse {
      throw new IllegalArgumentException(s"Invalid column name $name")
    }
  }

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

