package athena.client

import athena.data.{ColumnDef, CValue}
import akka.util.ByteString
import scala.collection.mutable

class Row private (columnDefs: IndexedSeq[ColumnDef], data: IndexedSeq[ByteString]) {

  //internally used to cache the columnName -> index lookup
  private[this] val indexMap: mutable.Map[String, Int] = mutable.Map()

  val columns = columnDefs

  def value(index: Int): CValue = CValue.parse(columnDefs(index).dataType, data(index))
  def value(name: String): CValue = value(indexOfColumn(name))
  def values: Iterator[CValue] = columnDefs.zip(data).iterator.map {
    case (columnDef, columnData) => CValue.parse(columnDef.dataType, columnData)
  }

  //TODO - add a 'rawValue' method that returns a CValue that just wraps the bytestring and data type
  //this will allow people to access the raw unparsed bytes if desired

  private def indexOfColumn(name: String): Int = {
    indexMap.getOrElseUpdate(name, {
      val index = columnDefs.indexWhere(_.name.equalsIgnoreCase(name))
      if(index < 0) {
        throw new IllegalArgumentException(s"Unknown column $name")
      }
      index
    })
  }

  override def toString: String = {
    val nameTuples = values.zip(columnDefs.iterator.map(_.name)).map(t => s"${t._2} = ${t._1}").mkString(", ")
    s"Row($nameTuples)"
  }
}

object Row {
  def apply(columnDefs: IndexedSeq[ColumnDef], data: IndexedSeq[ByteString]) = {
    if(columnDefs.size != data.size) {
      throw new IllegalArgumentException("Must have a column def for each data element.")
    }
    new Row(columnDefs, data)
  }
}
