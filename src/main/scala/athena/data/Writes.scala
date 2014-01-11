package athena.data

import scala.annotation.implicitNotFound
import org.joda.time.DateTime
import java.util.{UUID, Date}

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

}

trait DefaultWrites {

  implicit object StringWrites extends Writes[String] {
    def writes(value: String): CValue = CVarChar(value)
  }

  implicit object IntWrites extends Writes[Int] {
    def writes(value: Int): CValue = CInt(value)
  }

  implicit object DoubleWrites extends Writes[Double] {
    def writes(value: Double): CValue = CDouble(value)
  }

  implicit object DateTimeWrites extends Writes[DateTime] {
    def writes(value: DateTime): CValue = CTimestamp(value)
  }

  implicit object DateWrites extends Writes[Date] {
    def writes(value: Date): CValue = CTimestamp(new DateTime(value))
  }

  implicit object UUIDWrites extends Writes[UUID] {
    def writes(value: UUID): CValue = CUUID(value)
  }

  implicit def mapWrites[A, B](implicit a: Writes[A], b: Writes[B]): Writes[Map[A, B]] = new Writes[Map[A, B]] {
    def writes(value: Map[A, B]): CValue = CMap(value.map {
      case (key, value) => (CValue(key), CValue(value))
    })
  }

  implicit def listWrites[A](implicit a: Writes[A]): Writes[List[A]] = new Writes[List[A]] {
    def writes(value: List[A]): CValue = CList(value.map(CValue(_)))
  }

  implicit def setWrites[A](implicit a: Writes[A]): Writes[Set[A]] = new Writes[Set[A]] {
    def writes(value: Set[A]): CValue = CSet(value.map(CValue(_)))
  }

  implicit def optionWrites[A](implicit a: Writes[A]): Writes[Option[A]] = new Writes[Option[A]] {
    def writes(value: Option[A]): CValue = value.map(CValue(_)).getOrElse(CNull)
  }

}

