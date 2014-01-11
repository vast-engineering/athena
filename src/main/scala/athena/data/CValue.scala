package athena.data

import akka.util.ByteString
import java.net.InetAddress
import java.util.{Date, UUID}
import java.math.{ BigInteger => JBigInteger, BigDecimal => JBigDecimal}
import org.joda.time.DateTime

import athena.data.NativeDataType._
import java.nio.ByteOrder

case class ConversionException(errors: Seq[String]) extends RuntimeException(s"Problem converting value - ${errors.mkString(", ")}")

/**
 * A generic value returned or sent as a parameter to or from a Cassandra query.
 */
sealed trait CValue {

  /**
   * Tries to convert the node into a T, throwing an exception if it can't. An implicit Reads[T] must be defined.
   */
  def as[T](implicit fjs: Reads[T]): T = fjs.reads(this).fold(
    valid = identity,
    invalid = e => throw new ConversionException(e)
  )

  /**
   * Tries to convert the node into a JsResult[T] (Success or Error). An implicit Reads[T] must be defined.
   */
  def validate[T](implicit rds: Reads[T]): CvResult[T] = rds.reads(this)

  def prettyPrint = this.toString.take(100)

  //JAVA API
  //protected[CValue] means private for Scala, public for Java
  protected[CValue] def as[T](clazz: Class[T]): T = {
    import ClassIdentifiers._
    val result = clazz match {
      case StringC => this.as[String]
      case JIntC => this.as[Int]
      case JDoubleC => this.as[Double]
      case DateTimeC => this.as[DateTime]
      case DateC => this.as[Date]
      case UUIDC => this.as[UUID]
      case _ => throw new ConversionException(Seq(s"Cannot convert value $this to type $clazz"))
    }
    result.asInstanceOf[T]
  }

}

object CValue {

  def apply[T: Writes](o: T) = toValue(o)

  /**
   * Provided a Reads implicit for its type is available, convert any object into a CValue.
   *
   * @param o Value to convert to a CValue.
   */
  def toValue[T](o: T)(implicit tjs: Writes[T]): CValue = {
    //shouldn't really have to check for this in a perfect world, but whatever
    if(o == null) {
      CNull
    } else {
      tjs.writes(o)
    }
  }

  /**
   * Provided a Writes implicit for that type is available, convert a CValue to any type.
   *
   * @param cvalue CValue to transform as an instance of T.
   */
  def fromValue[T](cvalue: CValue)(implicit fjs: Reads[T]): CvResult[T] = fjs.reads(cvalue)

  def parse(dataType: DataType, data: ByteString): CValue = {

    implicit val byteOrder = ByteOrder.BIG_ENDIAN

    if(data.isEmpty) {
      CNull
    } else {
      dataType match {
        case typ: NativeDataType if data.isEmpty => CNull
        case IntType => CInt(data.iterator.getInt)
        case ASCIIType => CASCIIString(data.decodeString("ASCII"))
        case BigIntType => CBigInt(data.iterator.getLong)
        case BlobType => CBlob(data)
        case BooleanType => CBoolean(data.head != 0)
        case CounterType => CCounter(data.iterator.getLong)
        case DecimalType =>
          val it = data.iterator
          val scale = it.getInt
          val unscaledBytes = it.toArray
          CDecimal(new JBigDecimal(new JBigInteger(unscaledBytes), scale))
        case DoubleType => CDouble(data.iterator.getDouble)
        case FloatType => CFloat(data.iterator.getFloat)
        case TimestampType => CTimestamp(new DateTime(data.iterator.getLong))
        case UUIDType =>
          val it = data.iterator
          CUUID(new UUID(it.getLong, it.getLong))
        case VarCharType => CVarChar(data.utf8String)
        case VarIntType => CVarInt(new JBigInteger(data.toArray))
        case TimeUUIDType =>
          val it = data.iterator
          val uuid = new UUID(it.getLong, it.getLong)
          if(uuid.version() != 1) {
            throw new IllegalArgumentException(s"Error deserializing type 1 UUID: deserialized value $uuid represents a type ${uuid.version} UUID")
          }
          CTimeUUID(uuid)
        case InetAddressType => CInetAddress(InetAddress.getByAddress(data.toArray))
        case MapType(keyType, valueType) =>
          val pairs = ParamCodecUtils.unpackMapParam(data)
          val dataMap = pairs.map {
            case (keyBytes, valueBytes) => (parse(keyType, keyBytes), parse(valueType, valueBytes))
          }.toMap
          CMap(dataMap)
        case ListType(valueType) =>
          CList(ParamCodecUtils.unpackListParam(data).map(parse(valueType, _)))
        case SetType(valueType) =>
          CSet(ParamCodecUtils.unpackListParam(data).map(parse(valueType, _)).toSet)
        case CustomType(className) =>
          CCustomValue(className, data)
      }
    }
  }

  //
  // JAVA API
  //
  /**
   * Convert a value of any type to a CValue (if possible).
   *
   * If the value cannot be converted, a ConversionException will be thrown
   */
  protected[CValue] def fromAny(x: Any): CValue = {
    import ClassIdentifiers._
    import athena.data.Writes._

    x.getClass match {
      case StringC => StringWrites.writes(x.asInstanceOf[String])
      case JIntC => IntWrites.writes(x.asInstanceOf[Int])
      case JDoubleC => DoubleWrites.writes(x.asInstanceOf[Double])
      case DateTimeC => DateTimeWrites.writes(x.asInstanceOf[DateTime])
      case DateC => DateWrites.writes(x.asInstanceOf[Date])
      case _ => throw new ConversionException(Seq(s"Cannot convert value $x to a CValue - no converter found."))
    }
  }


}

//for non-collection types

case object CNull extends CValue

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

case class CMap(values: Map[CValue, CValue]) extends CCollectionValue
case class CList(values: Seq[CValue]) extends CCollectionValue
case class CSet(values: Set[CValue]) extends CCollectionValue

case class CCustomValue(className: String, data: ByteString) extends CValue

object CBoolean {
  private[athena] val TRUE_BUFFER = ByteString(Array[Byte](1))
  private[athena] val FALSE_BUFFER = ByteString(Array[Byte](0))
}

private object ClassIdentifiers {
  //a set of stable identifiers for some Class ojects
  val StringC = classOf[String]
  val JIntC = classOf[java.lang.Integer]
  val JDoubleC = classOf[java.lang.Double]
  val DateTimeC = classOf[DateTime]
  val DateC = classOf[Date]
  val UUIDC = classOf[UUID]
  val InetAddressC = classOf[InetAddress]
  val JMapC = classOf[java.util.Map[_, _]]
  val JListC = classOf[java.util.List[_]]
  val JSetC = classOf[java.util.Set[_]]
}



