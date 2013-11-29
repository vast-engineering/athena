package athena.data

import akka.util.ByteString
import java.net.InetAddress
import java.util.UUID
import java.math.{ BigInteger => JBigInteger, BigDecimal => JBigDecimal}
import org.joda.time.DateTime
import java.nio.ByteOrder

import scala.collection.mutable
import athena.util.ByteStringUtils
import athena.Athena

//this file contains wrappers for each of the defined Cassandra data types

sealed trait CValue {
  def as[A](implicit reader: ValueReader[A]): A = reader.read(this)
}

object CValue {

  import ByteStringUtils._

  def toByteString(cv: CValue)(implicit byteOrder: ByteOrder): ByteString = {
    cv match {
      case CASCIIString(value) => ByteString(value, "ASCII")
      case CBigInt(value) => toByteString(value)
      case CBlob(bytes) => bytes
      case CBoolean(value) => if(value) CBoolean.TRUE_BUFFER else CBoolean.FALSE_BUFFER
      case CCounter(value) => toByteString(value)
      case CDecimal(value) =>
        val scale = value.scale()
        val intBytes = value.unscaledValue().toByteArray
        newBuilder(4 + intBytes.length).putInt(scale).putBytes(intBytes).result()
      case CDouble(value) => newBuilder(8).putDouble(value).result()
      case CFloat(value) => newBuilder(4).putFloat(value).result()
      case CInt(value) => newBuilder(4).putInt(value).result()
      case CTimestamp(value) => newBuilder(8).putLong(value.getMillis).result()
      case CUUID(value) =>
        newBuilder(16)
          .putLong(value.getMostSignificantBits)
          .putLong(value.getLeastSignificantBits)
          .result()
      case CVarChar(value) => ByteString(value, "UTF-8")
      case CVarInt(value) => ByteString(value.toByteArray)
      case CTimeUUID(value) =>
        newBuilder(16)
          .putLong(value.getMostSignificantBits)
          .putLong(value.getLeastSignificantBits)
          .result()
      case CInetAddress(value) =>  ByteString(value.getAddress)
      case CMap(values) =>
        //mutability is okay here
        val length = values.size
        val strings = new mutable.ArrayBuffer[ByteString](length)
        values.foreach {
          case (key, value) =>
            strings.append(toByteString(key))
            strings.append(toByteString(value))
        }
        ParamCodecUtils.packParamByteStrings(strings, length)
      case CList(values) =>
        val converted = values.map(toByteString(_))
        ParamCodecUtils.packParamByteStrings(converted, converted.size)
      case CSet(values) =>
        val converted = values.map(toByteString(_))
        ParamCodecUtils.packParamByteStrings(converted, converted.size)
      case CCustomValue(_, _) => throw new Athena.InternalException("Custom values are not supported.")
    }
  }

  private[this] def toByteString(l: Long)(implicit byteOrder: ByteOrder): ByteString = newBuilder(8).putLong(l).result()

}

//for non-collection types
sealed abstract class CScalarValue extends CValue

case class CASCIIString(value: String) extends CScalarValue
case class CBigInt(value: Long) extends CScalarValue
case class CBlob(bytes: ByteString) extends CScalarValue
case class CBoolean(value: Boolean) extends CScalarValue
case class CCounter(value: Long) extends CScalarValue
case class CDecimal(value: JBigDecimal) extends CScalarValue
case class CDouble(value: Double) extends CScalarValue
case class CFloat(value: Float) extends CScalarValue
case class CInt(value: Int) extends CScalarValue
case class CTimestamp(value: DateTime) extends CScalarValue
case class CUUID(value: UUID) extends CScalarValue
case class CVarChar(value: String) extends CScalarValue
case class CVarInt(value: JBigInteger) extends CScalarValue
case class CTimeUUID(value: UUID) extends CScalarValue
case class CInetAddress(address: InetAddress) extends CScalarValue

sealed abstract class CCollectionValue extends CValue

case class CMap(values: Map[CScalarValue, CScalarValue]) extends CCollectionValue
case class CList(values: Seq[CScalarValue]) extends CCollectionValue
case class CSet(values: Set[CScalarValue]) extends CCollectionValue

case class CCustomValue(className: String, data: ByteString) extends CValue

object CBoolean {
  private[athena] val TRUE_BUFFER = ByteString(Array[Byte](1))
  private[athena] val FALSE_BUFFER = ByteString(Array[Byte](0))
}


