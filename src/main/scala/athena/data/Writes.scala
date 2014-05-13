package athena.data

import scala.annotation.implicitNotFound
import org.joda.time.DateTime
import java.util.{UUID, Date}
import play.api.libs.json.{JsValue, Json, Writes => JsonWrites}

/**
 * Cassandra serializer: write an implicit to define a marshaller for any type.
 * 
 * An instance of Writes is a class that knows how to turn a value of type A into a CValue.
 * 
 */
@implicitNotFound(
"No Cassandra marshaller found for type ${A}. Try to implement an implicit Writes or Format for this type."
)
trait Writes[-A] {
  def writes(value: A): CValue


}

object Writes extends DefaultWrites {

  def apply[A](f: A => CValue): Writes[A] = new Writes[A] {
    def writes(a: A): CValue = f(a)
  }

  def of[A](implicit w: Writes[A]): Writes[A] = w

}

trait DefaultWrites {

  implicit object StringWrites extends Writes[String] {
    def writes(value: String): CValue = CVarChar(value)
  }

  implicit object IntWrites extends Writes[Int] {
    def writes(value: Int): CValue = CInt(value)
  }

  implicit object LongWrites extends Writes[Long] {
    override def writes(value: Long): CValue = CBigInt(value)
  }

  implicit object DoubleWrites extends Writes[Double] {
    def writes(value: Double): CValue = CDouble(value)
  }

  implicit object FloatWrites extends Writes[Float] {
    override def writes(value: Float): CValue = CFloat(value)
  }

  implicit object DateTimeWrites extends Writes[DateTime] {
    def writes(value: DateTime): CValue = CTimestamp(value.getMillis)
  }

  implicit object DateWrites extends Writes[Date] {
    def writes(value: Date): CValue = CTimestamp(value.getTime)
  }

  implicit object UUIDWrites extends Writes[UUID] {
    def writes(value: UUID): CValue = CUUID(value)
  }

  implicit object JsValueWrites extends Writes[JsValue] {
    def writes(value: JsValue): CValue = CVarChar(Json.stringify(value))
  }

  implicit def mapWrites[A, B](implicit a: Writes[A], b: Writes[B]): Writes[Map[A, B]] = new Writes[Map[A, B]] {
    def writes(value: Map[A, B]): CValue = CMap(value.map {
      case (k, v) => (CValue(k), CValue(v))
    })
  }

  implicit def seqWrites[A](implicit a: Writes[A]): Writes[Seq[A]] = new Writes[Seq[A]] {
    def writes(value: Seq[A]): CValue = CList(value.map(CValue(_)))
  }

  implicit def setWrites[A](implicit a: Writes[A]): Writes[Set[A]] = new Writes[Set[A]] {
    def writes(value: Set[A]): CValue = CSet(value.map(CValue(_)))
  }

  implicit def optionWrites[A](implicit a: Writes[A]): Writes[Option[A]] = new Writes[Option[A]] {
    def writes(value: Option[A]): CValue = value.map(CValue(_)).getOrElse(CNull)
  }

  def jsonWrites[T](implicit jsWrites: JsonWrites[T]): Writes[T] = Writes[T](t => CValue.toValue(jsWrites.writes(t)))


}

