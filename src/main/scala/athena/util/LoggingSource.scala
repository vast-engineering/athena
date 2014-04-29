package athena.util

import akka.event.{Logging, LoggingAdapter}
import akka.actor.{ActorContext, ActorRefFactory, ActorSystem}

trait LoggingSource {
  def apply(category: String): LoggingAdapter
}

object LoggingSource {

  implicit def fromActorRefFactory(implicit refFactory: ActorRefFactory): LoggingSource =
    refFactory match {
      case x: ActorSystem  ⇒ fromSystem(x)
      case x: ActorContext ⇒ fromActorContext(x)
    }

  def fromActorContext(context: ActorContext) = fromSystem(context.system)
  def fromSystem(implicit system: ActorSystem): LoggingSource = new LoggingSource {
    override def apply(category: String): LoggingAdapter = Logging(system, category)
  }

}