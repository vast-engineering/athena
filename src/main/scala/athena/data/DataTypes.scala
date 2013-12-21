package athena.data

import akka.util.{ByteIterator, ByteString}
import java.math.{ BigInteger => JBigInteger, BigDecimal => JBigDecimal}
import org.joda.time.DateTime
import java.util.UUID
import java.net.InetAddress
import athena.Athena
import java.nio.ByteOrder

sealed trait DataType {
  def name: String
  override def toString: String = s"DataType($name)"
}

object DataType {

  private implicit val byteOrder = ByteOrder.BIG_ENDIAN

  def fromByteIterator(it: ByteIterator): DataType = {
    val typeId = it.getShort
    //ensure that the match compiles to an efficient table lookup
    (typeId: Int) match {
      case CUSTOM_TYPE_ID =>
        CustomType.fromByteIterator(it)
      case LIST_TYPE_ID =>
        val elementType = NativeDataType.fromId(it.getShort)
        ListType(elementType)
      case SET_TYPE_ID =>
        val elementType = NativeDataType.fromId(it.getShort)
        SetType(elementType)
      case MAP_TYPE_ID =>
        val keyType = NativeDataType.fromId(it.getShort)
        val valueType = NativeDataType.fromId(it.getShort)
        MapType(keyType, valueType)
      case id =>
        //means it's a native scalar type
        NativeDataType.fromId(id)
    }
  }

  private[this] val CUSTOM_TYPE_ID = 0
  private[this] val LIST_TYPE_ID = 32
  private[this] val MAP_TYPE_ID = 33
  private[this] val SET_TYPE_ID = 34

}

case class CustomType(className: String) extends DataType {
  val name: String = s"CUSTOM($className)"
}
object CustomType {
  private implicit val byteOrder = ByteOrder.BIG_ENDIAN
  def fromByteIterator(it: ByteIterator): CustomType = CustomType(ParamCodecUtils.readString(it))
}

sealed abstract class CollectionType extends DataType
case class MapType(keyType: NativeDataType, valueType: NativeDataType) extends CollectionType {
  def name: String = s"MAP[${keyType.name}, ${valueType.name}]"
}
case class ListType(valueType: NativeDataType) extends CollectionType {
  def name: String = s"LIST[${valueType.name}]"
}
case class SetType(valueType: NativeDataType) extends CollectionType {
  def name: String = s"SET[${valueType.name}]"
}

sealed abstract class NativeDataType(val name: String) extends DataType

object NativeDataType {

  private implicit val byteOrder = ByteOrder.BIG_ENDIAN

  def fromId(id: Int): NativeDataType = {
    nativeTypeMap.get(id).getOrElse {
      throw new Athena.InternalException(s"Invalid type code $id returned from server - expected a native type ID.")
    }
  }

  private[this] val nativeTypeMap: Map[Int, NativeDataType] = Map(
    1 -> ASCIIType,
    2 -> BigIntType,
    3 -> BlobType,
    4 -> BooleanType,
    5 -> CounterType,
    6 -> DecimalType,
    7 -> DoubleType,
    8 -> FloatType,
    9 -> IntType,
    11 -> TimestampType,
    12 -> UUIDType,
    13 -> VarCharType,
    14 -> VarIntType,
    15 -> TimeUUIDType,
    16 -> InetAddressType
  )


  object ASCIIType extends NativeDataType("ASCII")
  object BigIntType extends NativeDataType("BIGINT")
  object BlobType extends NativeDataType("BLOB")
  object BooleanType extends NativeDataType("BOOLEAN")
  object CounterType extends NativeDataType("COUNTER")
  object DecimalType extends NativeDataType("DECIMAL")
  object DoubleType extends NativeDataType("DOUBLE")
  object FloatType extends NativeDataType("FLOAT")
  object IntType extends NativeDataType("INT")
  object TimestampType extends NativeDataType("TIMESTAMP")
  object UUIDType extends NativeDataType("UUID")
  object VarCharType extends NativeDataType("VARCHAR")
  object VarIntType extends NativeDataType("VARINT")
  object TimeUUIDType extends NativeDataType("TIMEUUID")
  object InetAddressType extends NativeDataType("INET")
}


