package athena.connector

import akka.actor._
import athena.Athena
import scala.concurrent.duration.{FiniteDuration, Duration}
import akka.actor.Terminated
import java.util.concurrent.TimeUnit

/**
 * This is an actor that cleanly closes either a cluster, pool, or host connection.
 */
private[connector] class CloseActor(connection: ActorRef, closeCmd: Athena.CloseCommand, timeout: FiniteDuration) extends Actor with ActorLogging {

  log.debug("Closing {} with command {}", connection, closeCmd)

  context.watch(connection)
  import context.dispatcher
  context.system.scheduler.scheduleOnce(timeout) {
    self ! 'killConnection
  }

  connection ! closeCmd

  def receive: Receive = {
    case evt: Athena.ConnectionClosed =>
      //everything went okay
      log.debug("Got closed message from actor.")
      //note - we don't stop the connection here, the assumption is that it will stop on it's own
      //we do give it a reasonable time to stop - if it doesn't, we kill it manually.

    case Terminated(`connection`) =>
      log.debug("Connection actor terminated")
      context.stop(self)

    case 'killConnection =>
      log.warning("Connection actor did not terminate within specified timeout interval. Killing actor.")
      context.unwatch(connection)
      context.stop(connection)
      context.stop(self)

  }
}

private[connector] object CloseActor {
  def props(connection: ActorRef, closeCmd: Athena.CloseCommand, timeout: Duration): Props = {
    val finiteTimeout = if(timeout.isFinite()) {
      FiniteDuration(timeout.toMillis, TimeUnit.MILLISECONDS)
    } else {
      FiniteDuration(5, TimeUnit.SECONDS)
    }
    Props(new CloseActor(connection, closeCmd, finiteTimeout))
  }
}
