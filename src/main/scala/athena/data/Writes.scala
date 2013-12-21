package athena.data

import scala.annotation.implicitNotFound
import org.joda.time.DateTime
import java.util.Date

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

  object StringWrites extends Writes[String] {
    def writes(value: String): CValue = CVarChar(value)
  }

  object IntWrites extends Writes[Int] {
    def writes(value: Int): CValue = CInt(value)
  }

  object DoubleWrites extends Writes[Double] {
    def writes(value: Double): CValue = CDouble(value)
  }

  object DateTimeWrites extends Writes[DateTime] {
    def writes(value: DateTime): CValue = CTimestamp(value)
  }

  object DateWrites extends Writes[Date] {
    def writes(value: Date): CValue = CTimestamp(new DateTime(value))
  }


}

