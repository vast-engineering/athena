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
  def decode(bs: ByteString): CValue
}

sealed abstract class NativeDataType(val name: String) extends DataType {
  override def decode(bs: ByteString): CScalarValue
}
sealed abstract class CollectionType extends DataType

case class CustomType(className: String) extends DataType {
  val name: String = "CUSTOM"

  def decode(bs: ByteString): CValue = CCustomValue(className, bs)
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
        listType(elementType)
      case SET_TYPE_ID =>
        val elementType = NativeDataType.fromId(it.getShort)
        setType(elementType)
      case MAP_TYPE_ID =>
        val keyType = NativeDataType.fromId(it.getShort)
        val valueType = NativeDataType.fromId(it.getShort)
        mapType(keyType, valueType)
      case id =>
        //means it's a native scalar type
        NativeDataType.fromId(id)
    }
  }

  private[this] def mapType(keyType: NativeDataType, valueType: NativeDataType): CollectionType = new CollectionType {
    def name: String = s"MAP[${keyType.name}, ${valueType.name}]"

    def decode(bs: ByteString): CValue = {
      val pairs = ParamCodecUtils.unpackMapParam(bs)
      val dataMap = pairs.map {
        case (keyBytes, valueBytes) => (keyType.decode(keyBytes), valueType.decode(valueBytes))
      }.toMap
      CMap(dataMap)
    }
  }

  private[this] def listType(valueType: NativeDataType): CollectionType = new CollectionType {
    def name: String = s"LIST[${valueType.name}]"

    def decode(bs: ByteString): CValue = CList(ParamCodecUtils.unpackListParam(bs).map(valueType.decode))
  }

  private[this] def setType(valueType: NativeDataType): CollectionType = new CollectionType {
    def name: String = s"SET[${valueType.name}]"

    def decode(bs: ByteString): CValue = CSet(ParamCodecUtils.unpackListParam(bs).map(valueType.decode).toSet)
  }

  private[this] val CUSTOM_TYPE_ID = 0
  private[this] val LIST_TYPE_ID = 32
  private[this] val MAP_TYPE_ID = 33
  private[this] val SET_TYPE_ID = 34

}

object CustomType {
  private implicit val byteOrder = ByteOrder.BIG_ENDIAN

  def fromByteIterator(it: ByteIterator): CustomType = CustomType(ParamCodecUtils.readString(it))
}

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


  object ASCIIType extends NativeDataType("ASCII") {
    def decode(bs: ByteString): CScalarValue = CASCIIString(bs.decodeString("ASCII"))
  }

  object BigIntType extends NativeDataType("BIGINT") {
    def decode(bs: ByteString): CScalarValue = CBigInt(bs.iterator.getLong)
  }

  object BlobType extends NativeDataType("BLOB") {
    def decode(bs: ByteString): CScalarValue = CBlob(bs)
  }

  object BooleanType extends NativeDataType("BOOLEAN") {
    def decode(bs: ByteString): CScalarValue = CBoolean(bs.head != 0)
  }

  object CounterType extends NativeDataType("COUNTER") {
    def decode(bs: ByteString): CScalarValue = CCounter(bs.iterator.getLong)
  }

  object DecimalType extends NativeDataType("DECIMAL") {
    def decode(bs: ByteString): CScalarValue = {
      val it = bs.iterator
      val scale = it.getInt
      val unscaledBytes = it.toArray
      CDecimal(new JBigDecimal(new JBigInteger(unscaledBytes), scale))
    }
  }

  object DoubleType extends NativeDataType("DOUBLE") {
    def decode(bs: ByteString): CScalarValue = CDouble(bs.iterator.getDouble)
  }

  object FloatType extends NativeDataType("FLOAT") {
    def decode(bs: ByteString): CScalarValue = CFloat(bs.iterator.getFloat)
  }

  object IntType extends NativeDataType("INT") {
    def decode(bs: ByteString): CScalarValue = CInt(bs.iterator.getInt)
  }

  object TimestampType extends NativeDataType("TIMESTAMP") {
    def decode(bs: ByteString): CScalarValue = CTimestamp(new DateTime(bs.iterator.getLong))
  }

  object UUIDType extends NativeDataType("UUID") {
    def decode(bs: ByteString): CScalarValue = {
      val it = bs.iterator
      CUUID(new UUID(it.getLong, it.getLong))
    }
  }

  object VarCharType extends NativeDataType("VARCHAR") {
    def decode(bs: ByteString): CScalarValue = CVarChar(bs.utf8String)
  }

  object VarIntType extends NativeDataType("VARINT") {
    def decode(bs: ByteString): CScalarValue = CVarInt(new JBigInteger(bs.toArray))
  }

  object TimeUUIDType extends NativeDataType("TIMEUUID") {
    def decode(bs: ByteString): CScalarValue = {
      val it = bs.iterator
      val uuid = new UUID(it.getLong, it.getLong)
      if(uuid.version() != 1) {
        throw new IllegalArgumentException(s"Error deserializing type 1 UUID: deserialized value $uuid represents a type ${uuid.version} UUID")
      }
      CTimeUUID(uuid)
    }
  }

  object InetAddressType extends NativeDataType("INET") {
    def decode(bs: ByteString): CScalarValue = CInetAddress(InetAddress.getByAddress(bs.toArray))
  }

}


