package athena.client

import athena.data.{CvResult, CValue, Writes}
import scala.annotation.implicitNotFound
import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import scala.concurrent.{ExecutionContext, Future}

@implicitNotFound("No implicit ParamGenerator was found for type ${T}. The easiest way to fix this is to ensure that a Writes value for ${T} is implicitly available.")
trait ParamGenerator[T] {
  def toParam: CValue
}

object ParamGenerator {
  import scala.language.implicitConversions

  implicit def fromWrites[T](t: T)(implicit w: Writes[T]): ParamGenerator[T] = new ParamGenerator[T] {
    override def toParam: CValue = w.writes(t)
  }
}


object QueryInterpolation {
  implicit class QueryContext(val s: StringContext) extends AnyVal {
    def cql(paramGenerators: ParamGenerator[_]*) = {
      val params = paramGenerators.map(_.toParam)
      val query = s.parts.mkString("?")
      new InterpolatedQuery(query, params)
    }
  }

  class InterpolatedQuery private[QueryInterpolation] (query: String, args: Seq[CValue])  {
    def as[A](implicit rr: RowReader[A], ec: ExecutionContext): StatementRunner[CvResult[A]] = new StatementRunner[CvResult[A]] {
      override def execute(implicit session: Session): Enumerator[CvResult[A]] = session.executeStream(query, args) &> Enumeratee.map(rr.read)
    }
  }

}

trait StatementRunner[A] {
  self =>

  def execute(implicit session: Session): Enumerator[A]

  def headOption(implicit session: Session, ec: ExecutionContext): Future[Option[A]] =
    execute |>>> Iteratee.head

  def head(implicit session: Session, ec: ExecutionContext): Future[A] =
    headOption.map(_.getOrElse(throw new NoSuchElementException("No results for statement.")))

  def map[B](f: A => B)(implicit session: Session, ec: ExecutionContext): StatementRunner[B] =
    new StatementRunner[B] {
      override def execute(implicit session: Session): Enumerator[B] =
        self.execute &> Enumeratee.map(f)
    }

  def foreach(f: A => Unit)(implicit session: Session, ec: ExecutionContext): Future[Unit] = execute |>>> Iteratee.foreach(f)

  def filter(f: A => Boolean)(implicit session: Session, ec: ExecutionContext): StatementRunner[A] =
    new StatementRunner[A] {
      override def execute(implicit session: Session): Enumerator[A] =
        self.execute &> Enumeratee.filter(f)
    }

}

class CQLQuery private (query: String) extends (Seq[CValue] => StatementRunner[Row]) {
  override def apply(args: Seq[CValue]): StatementRunner[Row] = new StatementRunner[Row] {
    override def execute(implicit session: Session): Enumerator[Row] = session.executeStream(query, args)
  }
}
object CQLQuery {
  def apply(query: String): CQLQuery = new CQLQuery(query)
}

