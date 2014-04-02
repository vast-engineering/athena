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
    def cql(paramGenerators: ParamGenerator[_]*) = {
      val params = paramGenerators.map(_.toParam)
      val query = s.parts.mkString("?")
      new InterpolatedQuery(query, params)
    }
  }

  implicit class SimpleQuery(val q: String) extends AnyVal {
    def toQuery[A, B](implicit rw: RowWriter[A], rr: RowReader[B], ec: ExecutionContext): A => StatementRunner[CvResult[B]] = {
      a =>
        StatementRunner[CvResult[B]] { session =>
          session.executeStream(q, rw.write(a)) &> Enumeratee.map(rr.read)
        }
    }
  }

  class InterpolatedQuery private[QueryInterpolation] (query: String, args: Seq[CValue])  {
    def as[A](implicit rr: RowReader[A], ec: ExecutionContext): StatementRunner[CvResult[A]] = StatementRunner[CvResult[A]] { session =>
      session.executeStream(query, args) &> Enumeratee.map(rr.read)
    }
  }

}


