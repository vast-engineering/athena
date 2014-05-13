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

  private[this] var connectors: Map[ClusterConnectorSetup, ActorRef] = Map.empty

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
      val connector = clusterConnectorFor(setup.normalized)
      connectCommander.tell(Athena.ClusterConnectorInfo(connector, setup), connector)
      connector ! Athena.AddClusterStatusListener(connectCommander)

    case Terminated(child) =>
      if (connectors.exists(_._2 == child)) {
        connectors = connectors.filter(_._2 != child)
      } else {
        log.warning("Got terminated message for unexpected Actor! {}", child)
      }

  }

  private def clusterConnectorFor(normalizedSetup: ClusterConnectorSetup): ActorRef = {
    def createConnector(): ActorRef = {
      log.debug("Creating new cluster connector for {}", normalizedSetup)
      context.actorOf(
        props = ClusterConnector.props(normalizedSetup),
        name = "cluster-connector-" + clusterConnectorCounter.next()
      )
    }
    def createAndRegisterHostConnector() = {
      val connector = createConnector
      connectors = connectors.updated(normalizedSetup, connector)
      context.watch(connector)
    }
    if(normalizedSetup.useExisting) {
      connectors.getOrElse(normalizedSetup, createAndRegisterHostConnector())
    } else {
      //create a brand new connector
      createConnector()
    }
  }



}
