package athena.connector

import athena.Athena
import akka.actor.{Actor, ActorLogging}

private[athena] class ClusterConnector(setup: Athena.ClusterConnectorSetup) extends Actor with ActorLogging  {
  def receive: Actor.Receive = {
    case x =>
  }
}
