package athena.client

import athena.data._
import scala.annotation.implicitNotFound

/**
 * A typeclass defining instances that know how to convert a single row from a Cassandra result into a type A
 */
@implicitNotFound(
  "An implicit instance of RowReader could not be found for the type ${A}. If A is a scalar type, ensure that a Reads is available for it. If it's a Tuple, ensure that a Reads is available for each member."
)
trait RowReader[A] {
  self =>
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

object RowReader extends DefaultRowReaders with ReaderBuilders {

  def apply[T](f: Row => CvResult[T]): RowReader[T] = new RowReader[T] {
    override def read(cvalue: Row): CvResult[T] = f(cvalue)
  }

}

trait DefaultRowReaders {

  import athena.client.util._

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

  implicit val unitReads: RowReader[Unit] = RowReader[Unit] { row => CvSuccess(()) }

  implicit def tupleReads[T, L <: HList](implicit ev: Tuple[T], tupler: Tupler.Aux[L, T], hlr: IndexedReaderSource[L]): RowReader[T] = {
    //start at index 0
    hlr(0).map[T](l => tupler(l))
  }

  implicit def singleReads[T](implicit r: Reads[T]): RowReader[T] = at[T](0)


  trait IndexedReaderSource[L <: HList] {
    def apply(index: Int): RowReader[L]
  }

  object IndexedReaderSource {
    implicit def nilRowReader: IndexedReaderSource[HNil] =
      new IndexedReaderSource[HNil] {
        override def apply(index: Int): RowReader[HNil] = RowReader[HNil](_ => CvSuccess(HNil))
      }

    implicit def indexedReader[H, T <: HList](implicit rh: Reads[H], rt: IndexedReaderSource[T]): IndexedReaderSource[H :: T] = {
      new IndexedReaderSource[H :: T] {
        override def apply(index: Int): RowReader[H :: T] = {
          val headReads = at[H](index)(rh)
          combine(headReads, rt(index + 1))
        }
      }
    }
  }

  protected def combine[H, T <: HList](rh: RowReader[H], rt: RowReader[T]): RowReader[H :: T] = {
    RowReader[H :: T] { row =>
      val head: CvResult[H] = rh.read(row)
      val tail: CvResult[T] = rt.read(row)
      (head, tail) match {
        case (CvError(leftError), CvError(rightError)) => CvError(leftError ++ rightError)
        case (left@CvError(_), _) => left
        case (_, right@CvError(_)) => right
        case (CvSuccess(h), CvSuccess(t)) =>
          CvSuccess(h :: t)
      }
    }
  }

}

trait ReaderBuilders {
  self: DefaultRowReaders =>

  import scala.language.implicitConversions
  import athena.client.util._

  class ReaderBuilder[T <: HList](rt: RowReader[T]) {
    //The :: operator is right associative, so that the syntax
    // at[Int]("foo") :: at[String]("bar") :: at[Long]("quux")
    // does the right thing in the most efficient manner
    // The expression above should yield a RowReader[Int :: String :: Long :: HNil]
    def ::[H](other: RowReader[H]): ReaderBuilder[H :: T] = new ReaderBuilder[H :: T](combine(other, rt))


    def hlisted: RowReader[T] = rt

    /**
    * Get a RowReader[A] where A is the tupled version of this ReaderBuilder's HList type parameter
    */
    def tupled(implicit tupler: Tupler[T]): RowReader[tupler.Out] = {
      rt.map(l => tupler(l))
    }

    /**
    * Accept a function of an arity that matches the types in this ReaderBuilder's type parameter
    * and produce a RowReader for the return type of the supplied function.
    */
    def apply[B, F](f: F)(implicit fnhlister: FnHLister.Aux[F, T => B]): RowReader[B] = {
      rt.map(fnhlister(f))
    }
  }

  implicit def toBuilder[L <: HList](r: RowReader[L]): ReaderBuilder[L] = new ReaderBuilder(r)
  implicit def scalarToBuilder[A](r: RowReader[A]): ReaderBuilder[A :: HNil] = new ReaderBuilder(r.map(x => x :: HNil))

}


