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


  //our actors do not restart
  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  def receive = {
    case connect: Athena.Connect =>
      val commander = sender()
      val settings = connect.settings.getOrElse(ConnectionSettings(context.system))
      context.actorOf(
        props = Props[ConnectionActor](new ConnectionActor(commander, connect.remoteAddress, settings, connect.initialKeyspace)),
        name = "connection-" + connectionCounter.next().toString)

    case setup: NodeConnectorSetup ⇒
      val normal = setup.normalized
      val commander = sender()
      val connector = context.actorOf(
        props = NodeConnector.props(commander, normal),
        name = "node-connector-" + nodeConnectorCounter.next())
      commander.tell(Athena.NodeConnectorInfo(connector, setup), connector)

    case setup: ClusterConnectorSetup =>
      val connectCommander = sender()
      val normal = setup.normalized
      val connector = context.actorOf(
        props = ClusterConnector.props(connectCommander, normal),
        name = "cluster-connector-" + clusterConnectorCounter.next()
      )
      connectCommander.tell(Athena.ClusterConnectorInfo(connector, setup), connector)
  }

}
