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
          case CvError(errors) => CvError(errors.map(error => s"Error reading column $index: value: $x error: $error"))
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
          case CvError(errors) => CvError(errors.map(error => s"Error reading column '$name': value: $x error: $error"))
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
  import ShapelessUtils._

  implicit def rowReaderReducer[H, T, Out0 <: HList](implicit append: Appender.Aux[H, T, Out0]): Reducer.Aux[RowReader, H, T, Out0] = new Reducer[RowReader, H, T] {
    type Out = Out0

    override def apply(mh: RowReader[H], mt: RowReader[T]): RowReader[Out] = RowReader[Out0] { row =>
      val head: CvResult[H] = mh.read(row)
      val tail: CvResult[T] = mt.read(row)
      (head, tail) match {
        case (CvError(leftError), CvError(rightError)) => CvError(leftError ++ rightError)
        case (left@CvError(_), _) => left
        case (_, right@CvError(_)) => right
        case (CvSuccess(h), CvSuccess(t)) =>
          CvSuccess(append(h, t))
      }
    }
  }

  implicit class RowReaderOps[A](val ra: RowReader[A]) extends AnyVal {
    //The :: operator is right associative, so that the syntax
    // at[Int]("foo") :: at[String]("bar") :: at[Long]("quux")
    // does the right thing in the most efficient manner
    // The expression above should yield a RowReader[Int :: String :: Long :: HNil]
    def ::[H, Out <: HList](rh: RowReader[H])(implicit reducer: Reducer.Aux[RowReader, H, A, Out]): RowReader[Out] = {
      reducer(rh, ra)
    }
  }

  implicit class HListReaderOps[L <: HList](val rl: RowReader[L]) extends AnyVal {
    def tupled(implicit tupler: Tupler[L]): RowReader[tupler.Out] = {
      rl.map(l => tupler(l))
    }
    def transform[A, F](f: F)(implicit fnhlister: FnHListerAux[F, L => A]): RowReader[A] = {
      rl.map(fnhlister(f))
    }
  }


  def read[A <: Product, B](f: B => A)(implicit r: RowReader[B]): RowReader[A] = r.map(f)

  implicit val unitReads: RowReader[Unit] = RowReader[Unit] { row => CvSuccess(()) }

  implicit def tupleReads[T <: Product, L <: HList](implicit tupler: TuplerAux[L, T], hlr: IndexedRowReader[L, _0]): RowReader[T] = {
    hlr.map[T]((l: L) => tupler(l))
  }

  implicit def hlistReads[L <: HList](implicit hlr: IndexedRowReader[L, _0]): RowReader[L] = hlr

  //A RowReader that will extract an HList of type L from a row, starting at the column indexed by N
  trait IndexedRowReader[L <: HList, N <: Nat] extends RowReader[L]

  object IndexedRowReader {
    implicit def nilRowReader[N <: Nat]: IndexedRowReader[HNil, N] =
      new IndexedRowReader[HNil, N] {
        override def read(cvalue: Row): CvResult[HNil] = CvSuccess(HNil)
      }

    implicit def hlistIndexedRowReader[H, T <: HList, N <: Nat](implicit rh: Reads[H], rt: IndexedRowReader[T, Succ[N]], ti: ToInt[N], reducer: Reducer.Aux[RowReader, H, T, H :: T]): IndexedRowReader[H :: T, N] = {
      val headReads = at[H](ti.apply())(rh)
      val reads = reducer(headReads, rt)
      new IndexedRowReader[H :: T, N] {
        override def read(cvalue: Row): CvResult[H :: T] = reads.read(cvalue)
      }

    }
  }
}
