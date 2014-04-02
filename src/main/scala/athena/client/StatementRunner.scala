package athena.client

import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import scala.concurrent.{ExecutionContext, Future}

trait StatementRunner[A] {
  self =>

  def stream(implicit session: Session): Enumerator[A]

  def execute(implicit session: Session): Future[Seq[A]]

  def headOption(implicit session: Session): Future[Option[A]]

  def head(implicit session: Session): Future[A]

  def foreach(f: A => Unit)(implicit session: Session): Future[Unit]

  def map[B](f: A => B): StatementRunner[B]

  def filter(f: A => Boolean): StatementRunner[A]
}

object StatementRunner {
  def apply[A](f: Session => Enumerator[A])(implicit ec: ExecutionContext): StatementRunner[A] = new StatementRunner[A] {
    self =>

    override def stream(implicit session: Session): Enumerator[A] = f(session)

    override def execute(implicit session: Session): Future[Seq[A]] = stream |>>> Iteratee.getChunks

    override def headOption(implicit session: Session): Future[Option[A]] = stream |>>> Iteratee.head

    override def head(implicit session: Session): Future[A] = headOption.map(_.getOrElse(throw new NoSuchElementException("No results for statement.")))

    override def foreach(f: A => Unit)(implicit session: Session): Future[Unit] = stream |>>> Iteratee.foreach(f)

    override def map[B](f: A => B): StatementRunner[B] = StatementRunner[B] { session =>
      self.stream(session) &> Enumeratee.map(f)
    }

    override def filter(f: A => Boolean): StatementRunner[A] = StatementRunner[A] { session =>
      self.stream(session) &> Enumeratee.filter(f)
    }
  }

}