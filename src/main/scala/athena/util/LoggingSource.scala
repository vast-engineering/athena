package athena.util

import akka.event.{Logging, LoggingAdapter}
import akka.actor.{ActorContext, ActorRefFactory, ActorSystem}

trait LoggingSource {
  def apply(category: String): LoggingAdapter
}

trait DefaultLoggingSource extends LSLowPriorityImplicits {

  import scala.language.implicitConversions

  implicit def fromActorRefFactory(implicit refFactory: ActorRefFactory): LoggingSource =
    fromSystem(spray.util.actorSystem(refFactory))


}

trait LSLowPriorityImplicits {
  self: DefaultLoggingSource =>

  implicit def fromSystem(implicit system: ActorSystem): LoggingSource = new LoggingSource {
    override def apply(category: String): LoggingAdapter = Logging(system, category)
  }
}

object LoggingSource extends DefaultLoggingSource