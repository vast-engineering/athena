package athena.data

import scala.annotation.implicitNotFound
import akka.util.ByteString
import athena.util.ByteStringUtils._
import java.nio.ByteOrder
import org.joda.time.DateTime
import java.util.{UUID, Date}
import java.net.InetAddress

import play.api.libs.json.{Reads => JsonReads, JsError, JsSuccess, Json, JsValue}
import com.fasterxml.jackson.core.JsonParseException
import scala.reflect.ClassTag

/**
 * A trait that defines a class that can convert from a type A value to another type B.
 */
@implicitNotFound(
"No Cassandra deserializer found for type ${A}. Try to implement an implicit Reads for this type."
)
trait Reads[A] {
  self =>

  /**
   * Convert the cassandra value into an A
   */
  def read(cvalue: CValue): CvResult[A]

  def map[B](f: A => B): Reads[B] =
    Reads[B] { cvalue => self.read(cvalue).map(f) }

  def flatMap[B](f: A => Reads[B]): Reads[B] = Reads[B] { cvalue =>
    self.read(cvalue).flatMap(t => f(t).read(cvalue))
  }

  def filter(f: A => Boolean): Reads[A] =
    Reads[A] { cvalue => self.read(cvalue).filter(f) }

  def filter(error: String)(f: A => Boolean): Reads[A] =
    Reads[A] { cvalue => self.read(cvalue).filter(error)(f) }

  def filterNot(f: A => Boolean): Reads[A] =
    Reads[A] { cvalue => self.read(cvalue).filterNot(f) }

  def filterNot(error: String)(f: A => Boolean): Reads[A] =
    Reads[A] { cvalue => self.read(cvalue).filterNot(error)(f) }

  def collect[B](error: String)(f: PartialFunction[A, B]) =
    Reads[B] { cvalue => self.read(cvalue).collect(error)(f) }

  def orElse(v: Reads[A]): Reads[A] =
    Reads[A] { cvalue => self.read(cvalue).orElse(v.read(cvalue)) }

  def compose[B <: CValue](rb: Reads[B]): Reads[A] =
    Reads[A] { value =>
      rb.read(value) match {
        case CvSuccess(b) => self.read(b)
        case CvError(e) => CvError(e)
      }
    }

}

object Reads extends DefaultReads with MetaReaders {
  def apply[A](f: CValue => CvResult[A]): Reads[A] = new Reads[A] {
    override def read(cvalue: CValue): CvResult[A] = f(cvalue)
  }
}

trait MetaReaders {

  def jsonReads[T](implicit jsReader: JsonReads[T]): Reads[T] = Reads[T] { cvalue =>
    cvalue.validate[JsValue].flatMap { jsValue =>
      jsReader.reads(jsValue) match {
        case JsSuccess(obj, _) => CvSuccess(obj)
        case JsError(errors) =>
          val cErrors = errors.map { error =>
            s"${error._1} -> ${error._2.mkString(", ")}"
          }
          CvError(cErrors)
      }
    }
  }

}

//trait ConstraintReads {
//  implicit def ofValue[A <: CValue](implicit ct: ClassTag[A]): Reads[A] = Reads[A] {
//    case x: A => CvSuccess(x)
//    case x => CvError(s"expected value of type ${ct.runtimeClass.getName}")
//  }
//
//}

trait DefaultReads {

//  implicit object CValueReads extends Reads[CValue] {
//    def reads(cvalue: CValue): CvResult[CValue] = CvSuccess(cvalue)
//  }

  implicit def optionReads[T](implicit rt: Reads[T]): Reads[Option[T]] = new Reads[Option[T]] {
      def read(cvalue: CValue): CvResult[Option[T]] = cvalue match {
        case CNull => CvSuccess(None)
        case x => rt.read(x).map(Some(_))
      }
  }

  import scala.reflect.runtime.universe._
  def nonNull[T](r: PartialFunction[CValue, CvResult[T]])(implicit t: TypeTag[T]): Reads[T] = {
    new Reads[T] {
      def read(cvalue: CValue): CvResult[T] = {
        cvalue match {
          case CNull => CvError(s"Cannot convert null value to type ${t.tpe.toString}.")
          case x => if(r.isDefinedAt(x)) r(x) else CvError(s"Cannot convert value to to type ${t.tpe.toString}.")
        }
      }
    }
  }

  def reads[T](r: PartialFunction[CValue, CvResult[T]])(implicit t: TypeTag[T]): Reads[T] = {
    new Reads[T] {
      def read(cvalue: CValue): CvResult[T] = {
        if(r.isDefinedAt(cvalue)) r(cvalue) else CvError(s"Cannot convert value to to type ${t.tpe.toString}.")
      }
    }
  }

  implicit val StringReads: Reads[String] = nonNull {
    case CVarChar(value) => CvSuccess(value)
    case CASCIIString(value) => CvSuccess(value)
    case CBigInt(value) => CvSuccess(value.toString)
    case CBoolean(value) => CvSuccess(value.toString)
    case CCounter(value) => CvSuccess(value.toString)
    case CDecimal(value) => CvSuccess(value.toString)
    case CDouble(value) => CvSuccess(value.toString)
    case CFloat(value) => CvSuccess(value.toString)
    case CInt(value) => CvSuccess(value.toString)
    case CTimestamp(value) => CvSuccess(value.toString)
    case CUUID(value) => CvSuccess(value.toString)
    case CVarInt(value) => CvSuccess(value.toString)
    case CTimeUUID(value) => CvSuccess(value.toString)
    case CInetAddress(address) => CvSuccess(address.getHostAddress)
  }

  implicit val BooleanReads: Reads[Boolean] = nonNull {
    case CBoolean(bool) => CvSuccess(bool)
  }

  implicit val IntReads: Reads[Int] = nonNull {
    case CInt(value) => CvSuccess(value)
  }

  implicit val LongReads: Reads[Long] = nonNull {
    case CInt(value) => CvSuccess(value)
    case CBigInt(value) => CvSuccess(value)
    case CCounter(value) => CvSuccess(value)
  }

  implicit val BigIntReads: Reads[java.math.BigInteger] = nonNull {
    case CVarInt(value) => CvSuccess(value)
  }

  implicit val DoubleReads: Reads[Double] = nonNull {
    case CInt(value) => CvSuccess(value.toDouble)
    case CDouble(value) => CvSuccess(value)
    case CFloat(value) => CvSuccess(value.toDouble)
  }

  implicit val BigDecimalReads: Reads[java.math.BigDecimal] = nonNull {
    case CDecimal(value) => CvSuccess(value)
  }

  implicit val JodaDateReads: Reads[DateTime] = nonNull {
    case CTimestamp(date) => CvSuccess(date)
  }

  implicit val DateReads: Reads[Date] = nonNull {
    case CTimestamp(date) => CvSuccess(date.toDate)
  }

  implicit val UUIDReads: Reads[UUID] = nonNull {
    case CUUID(value) => CvSuccess(value)
    case CTimeUUID(value) => CvSuccess(value)
  }

  implicit val InetAddressReads: Reads[InetAddress] = nonNull {
    case CInetAddress(address) => CvSuccess(address)
  }

  implicit val JsValueReads: Reads[JsValue] = {

    nonNull {
      case CASCIIString(data) =>
        try {
          CvSuccess(Json.parse(data))
        } catch {
          case e: JsonParseException => CvError(s"Could not parse value as JSON - ${e.getMessage}")
        }
      case CVarChar(data) =>
        try {
          CvSuccess(Json.parse(data))
        } catch {
          case e: JsonParseException => CvError(s"Could not parse value as JSON - ${e.getMessage}")
        }
      case CBlob(bytes) =>
        try {
          CvSuccess(Json.parse(bytes.toArray))
        } catch {
          case e: JsonParseException => CvError(s"Could not parse value as JSON - ${e.getMessage}")
        }
    }
  }

  implicit def mapReads[A, B](implicit keyReads: Reads[A], valueReads: Reads[B], a: TypeTag[A], b: TypeTag[B]): Reads[Map[A, B]] = reads {
    case CNull => CvSuccess(Map.empty)
    case CMap(values) =>
      val converted: Iterable[CvResult[(A, B)]] = values.map {
        case (key, value) => keyReads.read(key).zip(valueReads.read(value))
      }
      CvResult.sequence(converted).map(_.toMap)
  }

  implicit def seqReads[A](implicit valueReads: Reads[A], a: TypeTag[A]): Reads[Seq[A]] = reads {
    case CNull => CvSuccess(Seq.empty)
    case CList(values) =>
      CvResult.sequence(values.map(valueReads.read(_)))
  }

  implicit def setReads[A](implicit valueReads: Reads[A], a: TypeTag[A]): Reads[Set[A]] = reads {
    case CNull => CvSuccess(Set.empty)
    case CList(values) => CvResult.sequence(values.map(valueReads.read(_)).toSet)
    case CSet(values) => CvResult.sequence(values.map(valueReads.read(_)))
  }

  implicit object ByteStringReads extends Reads[ByteString] {

    private implicit val byteOrder = ByteOrder.BIG_ENDIAN
    /**
     * Convert the cassandra value into an A
     */
    def read(cvalue: CValue): CvResult[ByteString] = cvalue match {
      case CASCIIString(value) => CvSuccess(ByteString(value, "ASCII"))
      case CVarChar(value) => CvSuccess(ByteString(value, "UTF-8"))
      case CBigInt(value) => CvSuccess(newBuilder(8).putLong(value).result())
      case CBlob(bytes) => CvSuccess(bytes)
      case CBoolean(value) => CvSuccess(if(value) CBoolean.TRUE_BUFFER else CBoolean.FALSE_BUFFER)
      case CCounter(value) => CvSuccess(newBuilder(8).putLong(value).result())
      case CDecimal(value) =>
        val scale = value.scale()
        val intBytes = value.unscaledValue().toByteArray
        CvSuccess(newBuilder(4 + intBytes.length).putInt(scale).putBytes(intBytes).result())
      case CDouble(value) => CvSuccess(newBuilder(8).putDouble(value).result())
      case CFloat(value) => CvSuccess(newBuilder(4).putFloat(value).result())
      case CInt(value) => CvSuccess(newBuilder(4).putInt(value).result())
      case CTimestamp(value) => CvSuccess(newBuilder(8).putLong(value.getMillis).result())
      case CUUID(value) =>
        val uuid = newBuilder(16)
          .putLong(value.getMostSignificantBits)
          .putLong(value.getLeastSignificantBits)
          .result()
        CvSuccess(uuid)
      case CVarInt(value) => CvSuccess(ByteString(value.toByteArray))
      case CTimeUUID(value) =>
        val uuid = newBuilder(16)
          .putLong(value.getMostSignificantBits)
          .putLong(value.getLeastSignificantBits)
          .result()
        CvSuccess(uuid)
      case CInetAddress(value) =>  CvSuccess(ByteString(value.getAddress))
      case CMap(values) =>
        val length = values.size
        val strings = values.foldLeft(IndexedSeq.empty[CvResult[ByteString]]) {
          case (acc, (key, value)) => acc :+ CValue.fromValue[ByteString](key) :+ CValue.fromValue[ByteString](value)
        }
        CvResult.sequence(strings).map { byteStrings =>
          ParamCodecUtils.packParamByteStrings(byteStrings, length)
        }
      case CList(values) =>
        CvResult.sequence(values.map(CValue.fromValue[ByteString])).map { byteStrings =>
          ParamCodecUtils.packParamByteStrings(byteStrings, byteStrings.size)
        }
      case CSet(values) =>
        CvResult.sequence(values.map(CValue.fromValue[ByteString])).map { byteStrings =>
          ParamCodecUtils.packParamByteStrings(byteStrings, byteStrings.size)
        }
      case CCustomValue(_, _) => CvError("Custom values are not supported.")
      case CNull => CvSuccess(ByteString.empty)
    }
  }

  
}
