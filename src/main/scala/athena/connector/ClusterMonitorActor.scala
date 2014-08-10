package athena.connector

import athena.{Athena, ClusterConnectorSettings}
import akka.actor._
import akka.io.IO
import akka.pattern._

import java.net.{InetAddress, InetSocketAddress}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

import athena.client.pipelining._

import akka.util.Timeout

import athena.connector.CassandraResponses._
import akka.actor.Status.Failure
import athena.connector.CassandraResponses.NewNode
import scala.Some
import akka.actor.Terminated
import athena.connector.ClusterInfo.ClusterMetadata
import athena.connector.ClusterConnector.HostStatusChanged
import athena.Athena.NodeDisconnected
import athena.connector.ConnectionActor.ClusterEventsSubscribed

private[connector] class ClusterMonitorActor(commander: ActorRef, seedHosts: Set[InetAddress], port: Int, settings: ClusterConnectorSettings)
  extends Actor with ActorLogging with ClusterUtils {

  import ClusterMonitorActor._

  import context.system
  import context.dispatcher

  private val defaultTimeoutDuration = Duration(10, TimeUnit.SECONDS)
  private implicit val defaultTimeout = Timeout(defaultTimeoutDuration)

  private[this] val routingPolicy = new ReconnectionPolicy()

  private[this] val defaultBehavior: Receive = {

    case cmd: Athena.CloseCommand =>
      sender() ! cmd.event
      context.stop(self)

  }

  def receive = reconnect(seedHosts.map(a => a -> HostInfo(a)).toMap, true)

  def unconnected(hosts: Map[InetAddress, HostInfo]): Receive = {
    context.setReceiveTimeout(Duration.Inf)

    // TODO - eventually make this parameterizable
    val delay = defaultTimeoutDuration
    log.error("No host in cluster is reachable. Attempting reconnection in {}", delay)
    commander ! ClusterUnreachable

    //reschedule a connection attempt for 10 seconds from now
    context.system.scheduler.scheduleOnce(delay)(self ! 'reconnect)

    {
      case 'reconnect =>
        context.become(reconnect(hosts, true))

      case cmd: Athena.CloseCommand =>
        sender() ! cmd.event
        context.stop(self)

      case x =>
        log.warning("Received unknown message while unconnected. This shouldn't happen. {}", x)
    }
  }

  def reconnect(allHosts: Map[InetAddress, HostInfo], unconditional: Boolean = true): Receive = {

    log.debug("Attempting reconnection using hosts {}", allHosts)
    // TODO: Parameterize this
    context.setReceiveTimeout(defaultTimeoutDuration)

    def tryConnect(connectionHosts: IndexedSeq[HostInfo]): Receive = {

      if(connectionHosts.isEmpty) {
        unconnected(allHosts)
      } else {
        val host = connectionHosts.head
        log.debug("Attempting connection to {}", host.addr)
        val connectionSettings = settings.localNodeSettings.connectionSettings
        val address = new InetSocketAddress(host.addr, port)
        val connection = context.actorOf(ConnectionActor.props(address, connectionSettings))

        {
          case Athena.Connected(remote, _) if sender() == connection =>
            log.debug("Cluster monitor connected to {}", remote)
            if(unconditional) {
              //if unconditional is true, that means that all hosts were previously exhausted
              //we should tell the cluster manager to immediately attempt a reconnect
              commander ! ClusterReconnected
            }
            connection ! ConnectionActor.SubscribeToEvents
            context.become {
              case ClusterEventsSubscribed =>
                log.debug("Subscribed to cluster events. Moving to connected.")
                context.become(connected(connection, host, allHosts))
              case ReceiveTimeout =>
                log.warning("Timed out waiting for subscription. Closing connection.")
                context.actorOf(CloseActor.props(connection, Athena.Close, settings.localNodeSettings.closeTimeout))
                context.become(tryConnect(connectionHosts.tail))
            }

          case Athena.ConnectionFailed(remoteHost, error) if sender() == connection =>
            if(error.isDefined) {
              log.warning("Cluster connector cannot connect to host {} due to error {}", connectionHosts.head.addr, error.get)
              log.warning("Trying next host.")
            } else {
              log.debug("Connection to host {} failed, trying next host.", connectionHosts.head.addr)
            }
            context.become(tryConnect(connectionHosts.tail))

          case ReceiveTimeout =>
            log.debug("Connection attempt to host {} timed out, trying next host.", connectionHosts.head.addr)
            context.stop(connection)
            context.become(tryConnect(connectionHosts.tail))

          case cmd: Athena.CloseCommand =>
            sender() ! cmd.event
            context.stop(connection)
            context.stop(self)

        }
      }
    }

    tryConnect(routingPolicy.newPlan(allHosts.values, unconditional))
  }

  def connected(connection: ActorRef, connectedHost: HostInfo, initialHosts: Map[InetAddress, HostInfo]): Receive = {

    log.debug("Transitioning to connected state.")
    context.setReceiveTimeout(Duration.Inf)

    context.watch(connection)
    val pipeline = queryPipeline(connection)

    def whileConnected(hosts: Map[InetAddress, HostInfo]): Receive = {
      case info: ClusterMetadata =>
        log.debug("Cluster metadata received.")
        val newHosts = mergeHostInfo(info, hosts)
        //send the metadata to our commander
        commander ! info
        context.become(whileConnected(newHosts))

      case Failure(e) =>
        log.error(e, "Cluster metadata failed. Disconnecting.")
        //TODO - depending on the failure, we may want to just stop ourselves
        context.unwatch(connection)
        context.actorOf(CloseActor.props(connection, Athena.Close, settings.localNodeSettings.closeTimeout))
        context.become(reconnect(hosts))

      case Terminated(`connection`) =>
        log.warning("Connection to {} closed. Trying next host.", connectedHost)
        context.become(reconnect(hosts, false))

      case NodeDisconnected(addr) if addr == connectedHost.addr =>
        context.unwatch(connection)
        context.actorOf(CloseActor.props(connection, Athena.Close, settings.localNodeSettings.closeTimeout))
        context.become(reconnect(hosts))

      case cmd: Athena.CloseCommand =>
        val closeCommander = sender()
        connection ! cmd
        context.setReceiveTimeout(Duration(10, TimeUnit.SECONDS))
        context.become {
          case Terminated(`connection`) =>
            closeCommander ! cmd.event
            context.stop(self)

          case ReceiveTimeout =>
            log.warning("Cluster connector timed out waiting for connection to close. Killing it.")
            context.stop(connection)
            closeCommander ! cmd.event
            context.stop(self)
        }

      case evt: ClusterEvent =>
        log.debug("Got cluster event {}", evt)
        evt match {
          case HostStatusEvent(addr, isUp) =>
            val host = hosts.get(addr.getAddress).map(_.copy(isUp = isUp)).getOrElse {
              //we have a host we don't know about - create a new host entry for us and
              //trigger a metadata update
              updateClusterInfo(connectedHost.addr, pipeline) pipeTo self
              HostInfo(addr.getAddress, isUp)
            }
            commander ! HostStatusChanged(addr.getAddress, isUp)
            context.become(whileConnected(hosts.updated(addr.getAddress, host)))

          case NewNode(socketAddr) =>
            val addr = socketAddr.getAddress
            updateClusterInfo(connectedHost.addr, pipeline) pipeTo self
            context.become(whileConnected(hosts.updated(addr, HostInfo(addr, true))))

          case NodeRemoved(socketAddr) =>
            val addr = socketAddr.getAddress
            updateClusterInfo(connectedHost.addr, pipeline) pipeTo self
            context.become(whileConnected(hosts - addr))

          case NodeMoved(_) =>
            updateClusterInfo(connectedHost.addr, pipeline) pipeTo self

          case e: SchemaEvent =>
            log.warning("Ignoring schema update event {}", e)
        }

      case x =>
        log.debug("Received message - {}", x)

    }

    def mergeHostInfo(meta: ClusterMetadata, hosts: Map[InetAddress, HostInfo]): Map[InetAddress, HostInfo] = {
      meta.hosts.keys.map { addr =>
        //the cluster metadata contains the canonical list - if we already have it in our
        //list, use it, if not, create a new entry
        val host = hosts.get(addr).getOrElse {
          log.debug("Adding new host {}.", addr)
          HostInfo(addr, true)
        }
        addr -> host
      }.toMap
    }

    //Do an initial cluster metadata refresh on ourselves
    updateClusterInfo(connectedHost.addr, pipeline) pipeTo self
    whileConnected(initialHosts)

  }

}

private[connector] object ClusterMonitorActor {

  private case class HostInfo(addr: InetAddress, isUp: Boolean = true)

  //sent to the cluster after a successful reconnection attempt after all hosts are unreachable
  case object ClusterReconnected
  case object ClusterUnreachable

  //a simple little stateful class that generates lists of hosts
  //in a round robin fashion
  //this is a stateful class, but since it's entirely referenced inside our Actor, it's okay
  private class ReconnectionPolicy {

    var index = 0

    def newPlan(hosts: Iterable[HostInfo], unconditional: Boolean): IndexedSeq[HostInfo] = {

      val planHosts: IndexedSeq[HostInfo] = (if(unconditional) {
        hosts
      } else {
        //if not unconditional, only attempt to access hosts marked as up
        hosts.filter(_.isUp)
      }).toIndexedSeq

      if(index > planHosts.size) index = 0
      val (tail, head) = planHosts.splitAt(index)
      index = index + 1
      head ++ tail
    }
  }

}

