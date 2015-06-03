package athena.client

import athena.client.util._
import athena.data.{Writes, CValue}

/**
 * A typeclass that can turn an input of type T into parameters suitable for a CQL query.
 */
trait RowWriter[T] {
  def write(t: T): Seq[CValue]
}

object RowWriter {

  def apply[T](f: T => Seq[CValue]): RowWriter[T] = new RowWriter[T] {
    override def write(t: T): Seq[CValue] = f(t)
  }

  import scala.language.implicitConversions

  implicit def fromWrites[T](implicit wt: Writes[T]) = RowWriter[T] { t =>
    Seq(wt.writes(t))
  }

  implicit def tupleWriter[T <: Product, L <: HList](implicit hlister: HLister.Aux[T, L], hlistWriter: RowWriter[L]): RowWriter[T] = RowWriter[T] { t =>
    hlistWriter.write(hlister(t))
  }

  implicit val nilWriter: RowWriter[HNil] = RowWriter[HNil] { l =>
    Nil
  }

  implicit def hlistWriter[H, T <: HList](implicit wh: Writes[H], wt: RowWriter[T]): RowWriter[H :: T] = RowWriter[H :: T] { l =>
   wh.writes(l.head) +: wt.write(l.tail)
  }

}
