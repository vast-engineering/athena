package athena.data

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

case class CvSuccess[T](value: T) extends CvResult[T] {
  def get: T = value
}

case class CvError(errors: Seq[String]) extends CvResult[Nothing] {
  def get: Nothing = throw new ConversionException(errors)
}

object CvError {
  def apply(): CvError = CvError(Seq())
  def apply(err: String): CvError = CvError(Seq(err))
} 

/**
 * A monadic type returned from marshalling/unmarshalling attempts. This can be used to indicate errors and chain
 * together conversion operations.
 */
sealed trait CvResult[+A] {
  self =>

  def fold[X](invalid: Seq[String] => X, valid: A => X): X = this match {
    case CvSuccess(v) => valid(v)
    case CvError(e) => invalid(e)
  }

  def map[X](f: A => X): CvResult[X] = this match {
    case CvSuccess(v) => CvSuccess(f(v))
    case e: CvError => e
  }

  def combine[B, X](that: CvResult[B])(combiner: (A, B) => X): CvResult[X] = (this, that) match {
    case (CvError(leftError), CvError(rightError)) => CvError(leftError ++ rightError)
    case (left: CvError, right: CvSuccess[B]) => left
    case (left: CvSuccess[A], right: CvError) => right
    case (CvSuccess(a), CvSuccess(b)) => CvSuccess(combiner(a, b))
  }

  def filterNot(error: String)(p: A => Boolean): CvResult[A] =
    this.flatMap { a => if (p(a)) CvError(error) else CvSuccess(a) }

  def filterNot(p: A => Boolean): CvResult[A] =
    this.flatMap { a => if (p(a)) CvError() else CvSuccess(a) }

  def filter(p: A => Boolean): CvResult[A] =
    this.flatMap { a => if (p(a)) CvSuccess(a) else CvError() }

  def filter(otherwise: String)(p: A => Boolean): CvResult[A] =
    this.flatMap { a => if (p(a)) CvSuccess(a) else CvError(otherwise) }

  def collect[B](otherwise: String)(p: PartialFunction[A, B]): CvResult[B] = flatMap {
    case t if p.isDefinedAt(t) => CvSuccess(p(t))
    case _ => CvError(otherwise)
  }

  def flatMap[X](f: A => CvResult[X]): CvResult[X] = this match {
    case CvSuccess(v) => f(v)
    case e: CvError => e
  }

  def zip[X](that: CvResult[X]): CvResult[(A, X)] = combine(that) { (left, right) => (left, right) }

  def foreach(f: A => Unit): Unit = this match {
    case CvSuccess(a) => f(a)
    case _ => ()
  }

  def get: A

  def getOrElse[AA >: A](t: => AA): AA = this match {
    case CvSuccess(a) => a
    case CvError(_) => t
  }

  def orElse[AA >: A](t: => CvResult[AA]): CvResult[AA] = this match {
    case s @ CvSuccess(_) => s
    case CvError(_) => t
  }

  def asOpt = this match {
    case CvSuccess(v) => Some(v)
    case CvError(_) => None
  }

  def recover[AA >: A](errManager: PartialFunction[CvError, AA]): CvResult[AA] = this match {
    case CvSuccess(v) => CvSuccess(v)
    case e: CvError => if (errManager isDefinedAt e) CvSuccess(errManager(e)) else this
  }
}

object CvResult {

  import scala.language.higherKinds

  /**
   * Transforms a `Traversable[CvResult[A]]` into a `CvResult[TraversableOnce[A]]`.
  *  Useful for reducing many `CvResult`s into a single `CvResult`.
    */
  def sequence[A, M[_] <: TraversableOnce[_]](in: M[CvResult[A]])(implicit cbf: CanBuildFrom[M[CvResult[A]], A, M[A]]): CvResult[M[A]] = {
    val zero: CvResult[mutable.Builder[A, M[A]]] = CvSuccess(cbf(in))
    in.foldLeft(zero) { (acc, res) =>
        acc.combine(res.asInstanceOf[CvResult[A]])( (builder, a) => builder += a )
    }.map(_.result())
  }
}