package athena.client

import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import scala.concurrent.{ExecutionContext, Future}
import athena.data.{CvResult, CValue}
import athena.util.Rate

trait StatementRunner[A] {
  def execute(implicit session: Session, ec: ExecutionContext): A
}

object StatementRunner {

  def unitRunner(query: String, args: Seq[CValue], usePreparedStatement: Boolean = true): StatementRunner[Future[Unit]] = new StatementRunner[Future[Unit]] {
    override def execute(implicit session: Session, ec: ExecutionContext): Future[Unit] = {
      if(usePreparedStatement) {
        session.prepare(query).flatMap { ps =>
          session.streamPrepared(ps, args).run(Iteratee.head).map(_ => ())
        }
      } else {
        session.executeStream(query, args).run(Iteratee.head).map(_ => ())
      }
    }
  }

  def streamRunner[A](query: String, args: Seq[CValue], usePreparedStatement: Boolean = true)(implicit rr: RowReader[A]) = new StatementRunner[Enumerator[CvResult[A]]] {
    override def execute(implicit session: Session, ec: ExecutionContext): Enumerator[CvResult[A]] = {
      if(usePreparedStatement) {
        Enumerator.flatten {
          session.prepare(query).map { ps =>
            session.streamPrepared(ps, args).map(rr.read)
          }
        }
      } else {
        session.executeStream(query, args).map(rr.read)
      }
    }
  }

  def seqRunner[A](query: String, args: Seq[CValue], usePreparedStatement: Boolean = true)(implicit rr: RowReader[A]) = new StatementRunner[Future[Seq[CvResult[A]]]] {
    override def execute(implicit session: Session, ec: ExecutionContext): Future[Seq[CvResult[A]]] = {
      if(usePreparedStatement) {
        session.prepare(query).flatMap { ps =>
          session.executePrepared(ps, args).map(rows => rows.map(rr.read))
        }
      } else {
        session.execute(query, args).map(rows => rows.map(rr.read))
      }
    }
  }

  def apply[A](f: Session => A): StatementRunner[A] = new StatementRunner[A] {
    override def execute(implicit session: Session, ec: ExecutionContext): A = f(session)
  }

  private def mapped[A](f: (Session, ExecutionContext) => A): StatementRunner[A] = new StatementRunner[A] {
    override def execute(implicit session: Session, ec: ExecutionContext): A = f(session, ec)
  }

  implicit class EnumeratorOps[A](val sr: StatementRunner[Enumerator[A]]) extends AnyVal {
    def headOption(implicit session: Session, ec: ExecutionContext): Future[Option[A]] = sr.execute |>>> Iteratee.head
    def head(implicit session: Session, ec: ExecutionContext): Future[A] = sr.execute |>>> Iteratee.head map(_.getOrElse(throw new NoSuchElementException("No results for statement.")))
    def foreach(f: A => Unit)(implicit session: Session, ec: ExecutionContext) = sr.execute |>>> Iteratee.foreach(f)
    def map[B](f: A => B): StatementRunner[Enumerator[B]] = StatementRunner.mapped[Enumerator[B]] { (session, ec) =>
      sr.execute(session, ec).map(f)(ec)
    }
  }

  implicit class SeqOps[A](val sr: StatementRunner[Future[Seq[A]]]) extends AnyVal {
    def headOption(implicit session: Session, ec: ExecutionContext): Future[Option[A]] = sr.execute.map(_.headOption)
    def head(implicit session: Session, ec: ExecutionContext): Future[A] = sr.execute.map(_.head)
    def foreach(f: A => Unit)(implicit session: Session, ec: ExecutionContext) = sr.execute.foreach(res => res.foreach(f))
    def map[B](f: A => B): StatementRunner[Future[Seq[B]]] = StatementRunner.mapped[Future[Seq[B]]] { (session, ec) =>
      sr.execute(session, ec).map(_.map(f))(ec)
    }
  }


}

