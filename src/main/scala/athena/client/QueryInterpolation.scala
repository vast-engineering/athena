package athena.client

import athena.Requests.BoundStatement
import athena.data.{PreparedStatementDef, CvResult, CValue, Writes}
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
      //don't use a prepared statement because there are no query parameters - doesn't make a lot of sense
      StatementRunner.streamRunner(q, Seq(), usePreparedStatement = false)(rr)
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

  implicit class PreparedStatementOps(val stmt: PreparedStatementDef) extends AnyVal {

    //TODO: Add bits that allow for changing query params like consistency, etc.
    // Probably need a more complex abstraction than this

    def binder[A](implicit rw: RowWriter[A]): A => BoundStatement = { a =>
      BoundStatement(stmt, rw.write(a))
    }

  }



}




