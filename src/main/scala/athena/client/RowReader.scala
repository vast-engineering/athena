package athena.client

import athena.data._
import scala.annotation.implicitNotFound

/**
 * A typeclass defining instances that know how to convert a single row from a Cassandra result into a type A
 */
@implicitNotFound(
  "An implicit instance of RowReader could not be found for the type ${A}. If A is a scalar type, ensure that a Reads is available for it. If it's a Tuple, ensure that a Reads is available for each member."
)
trait RowReader[A] { self =>
  def read(cvalue: Row): CvResult[A]

  def map[B](f: A => B): RowReader[B] = RowReader[B] { row =>
    self.read(row).map(f)
  }

  def flatMap[B](f: A => RowReader[B]): RowReader[B] = RowReader[B] { row =>
    self.read(row).flatMap(t => f(t).read(row))
  }

  def andThen[B](f: A => CvResult[B]): RowReader[B] = RowReader[B] { row =>
    self.read(row).flatMap(f)
  }

}

object RowReader {

  def apply[T](f: Row => CvResult[T]): RowReader[T] = new RowReader[T] {
    override def read(cvalue: Row): CvResult[T] = f(cvalue)
  }

  def of[T](implicit r: RowReader[T]) = r

  def at[T](index: Int)(implicit r: Reads[T]) = new RowReader[T] {
    override def read(cvalue: Row): CvResult[T] = {
      cvalue(index).map { x =>
        r.read(x).recover {
          case CvError(errors) => CvError(errors.map(x => s"Error reading column $index: $x"))
        }
      } getOrElse {
        CvError(s"Invalid column index $index")
      }
    }
  }

  def at[T](name: String)(implicit r: Reads[T]) = new RowReader[T] {
    override def read(cvalue: Row): CvResult[T] = {
      cvalue(name).map { x =>
        r.read(x).recover {
          case CvError(errors) => CvError(errors.map(x => s"Error reading column '$name': $x"))
        }
      } getOrElse {
        CvError(s"Invalid column $name")
      }
    }
  }

  implicit object IdentityReads extends RowReader[Row] {
    override def read(row: Row): CvResult[Row] = CvSuccess(row)
  }

  implicit def fromReads[T](implicit r: Reads[T]): RowReader[T] = at[T](0)

  import shapeless._
  import scala.language.implicitConversions
  import shapeless.Nat._0

  def read[A <: Product, B](f: B => A)(implicit r: RowReader[B]): RowReader[A] = r.map(f)

  implicit val unitReads: RowReader[Unit] = RowReader[Unit] { row => CvSuccess(()) }

  implicit def tupleReads[T <: Product, L <: HList](implicit tupler: TuplerAux[L, T], hlr: IndexedRowReader[L, _0]): RowReader[T] =
    hlr.map(l => tupler(l))

  implicit def hlistReads[L <: HList](implicit hlr: IndexedRowReader[L, _0]): RowReader[L] = hlr

  //A RowReader that will extract an HList of type L from a row, starting at the column indexed by N
  trait IndexedRowReader[L <: HList, N <: Nat] extends RowReader[L]

  object IndexedRowReader {
    implicit def nilRowReader[N <: Nat]: IndexedRowReader[HNil, N] =
      new IndexedRowReader[HNil, N] {
        override def read(cvalue: Row): CvResult[HNil] = CvSuccess(HNil)
      }

    implicit def hlistRowReader[H, T <: HList, N <: Nat](implicit rh: Reads[H], rt: IndexedRowReader[T, Succ[N]], ti: ToInt[N]): IndexedRowReader[H :: T, N] =
      new IndexedRowReader[H :: T, N] {
        val pathReads = at[H](ti.apply())(rh)
        override def read(row: Row): CvResult[::[H, T]] = {
          val head = pathReads.read(row)
          val tail = rt.read(row)
          (head, tail) match {
            case (CvError(leftError), CvError(rightError)) => CvError(leftError ++ rightError)
            case (left@CvError(_), _) => left
            case (_, right@CvError(_)) => right
            case (CvSuccess(a), CvSuccess(b)) =>
              CvSuccess(a :: b)
          }
        }
      }
  }
}
