package athena.client

import athena.data.{CvResult, CValue, Writes}
import scala.annotation.implicitNotFound
import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import scala.concurrent.{ExecutionContext, Future}

object QueryInterpolation {

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

  implicit class QueryContext(val s: StringContext) extends AnyVal {

    def i(args: Any*) = {

    }
    def cql(paramGenerators: ParamGenerator[_]*) = {
      val params = paramGenerators.map(_.toParam)
      val query = s.parts.mkString("?")
      new InterpolatedQuery(query, params)
    }

    def cqlu(paramGenerators: ParamGenerator[_]*) = {
      val params = paramGenerators.map(_.toParam)
      val query = s.parts.mkString("?")
      StatementRunner.unitRunner(query, params)
    }
  }

  implicit class SimpleQuery(val q: String) extends AnyVal {

    def asQuery[A, B](implicit rw: RowWriter[A], rr: RowReader[B]): A => StatementRunner[Enumerator[CvResult[B]]] = {
      a => StatementRunner.streamRunner(q, rw.write(a))(rr)
    }

    def asQueryNoArgs[B](implicit rr: RowReader[B]): StatementRunner[Enumerator[CvResult[B]]] = {
      StatementRunner.streamRunner(q, Seq())(rr)
    }

    def asUpdate[A](implicit rw: RowWriter[A]): A => StatementRunner[Future[Unit]] = {
      a => StatementRunner.unitRunner(q, rw.write(a))
    }
  }

  class InterpolatedQuery private[QueryInterpolation] (query: String, args: Seq[CValue]) {
    def as[A](implicit rr: RowReader[A]): StatementRunner[Enumerator[CvResult[A]]] = {
      StatementRunner.streamRunner(query, args)(rr)
    }
  }

}




