package athena.connector

import akka.actor._
import athena.Athena
import scala.concurrent.duration.Duration
import akka.actor.Terminated

/**
 * This is an actor that cleanly closes either a cluster, pool, or host connection.
 */
private[connector] class CloseActor(connection: ActorRef, closeCmd: Athena.CloseCommand, timeout: Duration) extends Actor with ActorLogging {

  //close actors should not restart - they are fire and forget, one and done
  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  context.watch(connection)
  context.setReceiveTimeout(timeout)
  connection ! closeCmd

  def receive: Receive = {
    case evt: Athena.ConnectionClosed =>
      //everything went okay
      context.stop(connection)
      context.stop(self)

    case Terminated(`connection`) =>
      //this is strange
      log.debug("Connection actor terminated before recieve of Closed event.")
      context.stop(self)

    case ReceiveTimeout =>
      log.warning("Connection actor did not close within specified timeout interval. Killing actor.")
      context.stop(connection)
      context.stop(self)

  }
}

private[connector] object CloseActor {
  def props(connection: ActorRef, closeCmd: Athena.CloseCommand, timeout: Duration): Props = Props(new CloseActor(connection, closeCmd, timeout))
}
