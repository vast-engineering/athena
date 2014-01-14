package athena

import akka.actor._
import athena.connector._
import athena.Athena.NodeConnectorSetup
import athena.Athena.ClusterConnectorSetup
import akka.actor.Terminated
import athena.Athena.ClusterConnectorSetup

/**
 * The toplevel API for the Athena driver - handles requests for new connections, node connectors and cluster connectors.
 *
 * @author David Pratt (dpratt@vast.com)
 */
private[athena] class AthenaManager(globalSettings: AthenaExt#Settings) extends Actor with ActorLogging {

  private[this] val connectionCounter = Iterator from 0
  private[this] val nodeConnectorCounter = Iterator from 0
  private[this] val clusterConnectorCounter = Iterator from 0

  def receive = {
    case connect: Athena.Connect =>
      val commander = sender
      val settings = connect.settings.getOrElse(ConnectionSettings(context.system))
      context.actorOf(
        props = Props[ConnectionActor](new ConnectionActor(commander, connect, settings)),
        name = connectionCounter.next().toString)

    case setup: NodeConnectorSetup ⇒
      val normal = setup.normalized
      val connector = context.actorOf(
        props = NodeConnector.props(sender, normal.remoteAddress, normal.keyspace, normal.settings.get),
        name = "node-connector-" + nodeConnectorCounter.next())
      sender.tell(Athena.NodeConnectorInfo(connector, setup), connector)

    case setup: ClusterConnectorSetup =>
      val normal = setup.normalized
      val connector = context.actorOf(
        props = ClusterConnector.props(sender, normal.initialHosts, normal.port, normal.keyspace, normal.settings.get),
        name = "cluster-connector-" + clusterConnectorCounter.next()
      )
      sender.tell(Athena.ClusterConnectorInfo(connector, setup), connector)
  }

}
