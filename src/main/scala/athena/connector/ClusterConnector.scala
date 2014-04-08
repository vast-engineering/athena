package athena.connector

import athena.{Responses, ClusterConnectorSettings, Athena}
import akka.actor._
import akka.pattern._
import athena.Requests.AthenaRequest
import athena.Responses.AthenaResponse
import scala.concurrent.duration.Duration
import athena.Athena._
import java.net.{InetSocketAddress, InetAddress}
import akka.event.LoggingAdapter
import athena.connector.ClusterMonitorActor.{ClusterUnreachable, ClusterReconnected}
import athena.Athena.NodeDisconnected
import athena.Responses.RequestFailed
import scala.Some
import athena.Athena.NodeConnected
import athena.Responses.ConnectionUnavailable
import akka.actor.Terminated
import athena.Responses.Timedout
import athena.connector.ClusterInfo.ClusterMetadata
import java.util.concurrent.TimeUnit

private[athena] class ClusterConnector(commander: ActorRef, setup: ClusterConnectorSetup) extends Actor with ActorLogging {

  import ClusterConnector._

  import context.dispatcher

  private[this] val settings = setup.settings.get

  //create and sign a death pact with the monitor - we need it to operate
  private[this] val clusterMonitor = context.watch {
    context.actorOf(
      props = Props(new ClusterMonitorActor(self, setup.initialHosts, setup.port, settings)),
      name = "cluster-monitor"
    )
  }

  private[this] val defaultRequestTimeout = settings.localNodeSettings.connectionSettings.requestTimeout

  private[this] val routingPlan = new RoutingPlan()

  private[this] var clusterMetadata: ClusterMetadata = ClusterMetadata(None, None, Map.empty)

  private[this] var pools: Map[InetAddress, ActorRef] = Map.empty
  //we keep this around as an indexedseq as an optimization for the query planner
  private[this] var liveHosts: IndexedSeq[ConnectedHost] = IndexedSeq.empty

  private[this] val actorNameIndex = Iterator from 0

  private[this] var statusListeners: Set[ActorRef] = Set(commander)

  private[this] var currentStatus: ClusterStatusEvent = ClusterDisconnected

  private[this] val defaultBehavior: Receive = {
    case req: AthenaRequest =>
      log.warning("Rejecting request due to default behavior")
      sender ! ConnectionUnavailable(req)

    case cmd: Athena.CloseCommand =>
      context.become(closing(cmd, Set(sender())))

    case ClusterUnreachable =>
      //let our commander know
      pools.values.foreach(_ ! Disconnect)
      updateStatus(ClusterDisconnected)

    case ClusterReconnected =>
      //this is sent by the cluster monitor after it has been disconnected from every host in the cluster
      //and has subsequently reconnected - we should tell all our pools to immediately attempt a reconnect
      pools.values.foreach(_ ! Reconnect)
      updateStatus(ClusterConnected)

      //Leave this commented out for now - this doesn't work when the cluster connector is created
      //externally and handed off (for example, the way Session does it)
      //this means we can theoretically 'leak' cluster connections, but it doesn't really happen in practice.
      //what we want is a way for a client to relinquish being the commander and hand it off to somebody else.
//    case Terminated(`commander`) =>
//      log.warning("Cluster commander unexpextedly terminated. Shutting down as well.")
//      context.become(closing(Athena.Abort, statusListeners))

    case Terminated(listener) if statusListeners.contains(listener) =>
      statusListeners = statusListeners - listener

    case AddClusterStatusListener =>
      val listener = sender()
      listener ! currentStatus
      context.watch(listener)
      statusListeners = statusListeners + listener

    case RemoveClusterStatusListener =>
      val listener = sender()
      context.unwatch(listener)
      statusListeners = statusListeners - listener

  }

  override def postStop(): Unit = {
    log.info("Cluster connector to {} shutting down.", clusterMetadata.name.getOrElse("Unknown"))
  }

  def receive = starting()

  private def starting(): Receive = {

    log.debug("Cluster connector starting.")

    val behavior: Receive = {
      case ClusterReconnected =>
        context.become(initializing)

      case ClusterUnreachable if setup.failOnInit =>
        updateStatus(ClusterFailed(GeneralError("Cluster is unreachable.")))
        context.stop(self) //boom

    }

    behavior orElse defaultBehavior

  }

  private def initializing: Receive = {

    log.debug("Cluster connector initializing.")

    val behavior: Receive = {

      case newMeta: ClusterMetadata =>
        //we should receive cluster metadata from the monitor actor
        //merging this metadata will cause our pools to be created and
        //connection attempts to fire.
        mergeMetadata(newMeta)

      case NodeConnected(addr) =>
        //wait for at least one node to connect before we route any requests
        hostUp(addr)
        context.become(running)

      case NodeFailed(addr, error) =>
        //this is a fatal error - we cannot do anything with this
        updateStatus(ClusterFailed(error))
        //now shut down
        context.become(closing(Athena.Abort, Set(commander)))

    }

    behavior orElse defaultBehavior
  }

  private def running: Receive = {

    log.info("Successfully connected to cassandra cluster '{}'", clusterMetadata.name.getOrElse("Unknown"))
    log.debug("Cluster connector running with hosts {}", pools.keySet)

    updateStatus(ClusterConnected)

    context.setReceiveTimeout(Duration.Inf)

    val behavior: Receive = {
      case req: AthenaRequest =>
        context.actorOf(
          props = RequestActor.props(req, sender(), routingPlan.generatePlan(req, liveHosts)(log), defaultRequestTimeout),
          name = "request-actor-" + actorNameIndex.next()
        )

      case newMeta: ClusterMetadata =>
        mergeMetadata(newMeta)

      case NodeConnected(addr) =>
        hostUp(addr)

      case x@NodeDisconnected(addr) =>
        //let the monitor know - in case it hasn't disconnected yet
        clusterMonitor ! x
        hostDown(addr)

      case HostStatusChanged(addr, isUp) =>
        //this is sent from the cluster monitor when the cluster signals a node is available
        togglePool(addr, isUp)

    }

    behavior orElse defaultBehavior
  }

  private def closing(closeCommand: Athena.CloseCommand, commanders: Set[ActorRef]): Receive = {

    context.setReceiveTimeout(settings.localNodeSettings.closeTimeout)

    log.info("Closing cluster connector with {} live pools.", pools.size)

    def step(closeActors: Set[ActorRef], commanders: Set[ActorRef]): Receive = {

      def signalDone() {
        commanders.foreach(_ ! closeCommand.event)
        context.stop(self)
      }

      if(closeActors.isEmpty) {
        signalDone()
        defaultBehavior
      } else {
        {
          case Terminated(closeActor) if closeActors.contains(closeActor) =>
            context.become(step(closeActors - closeActor, commanders))

          case req: AthenaRequest =>
            log.warning("Rejecting request because connector is shutting down.")
            sender ! Responses.RequestFailed(req)

          case cmd: Athena.CloseCommand =>
            log.debug("Ignoring close command {} - already shutting down.", cmd)
            context.become(step(closeActors, commanders + sender))

          case ReceiveTimeout =>
            log.warning("Timed out waiting for pools to close. Just stopping now.")
            signalDone()
        }
      }
    }

    val closeActors =  pools.values.map { pool =>
      context.watch(closePool(pool))
    }.toSet

    liveHosts = IndexedSeq.empty
    pools = Map.empty

    step(closeActors, commanders)
  }

  //called in reaction to a pool signalling that it's available
  private def hostUp(host: InetAddress, sendReconnect: Boolean = false) {
    pools.get(host).foreach { pool =>
      //this may be a new host - if we don't know about it yet, we don't add a pool
      //only bring it up after we've synched metadata
      if(liveHosts.exists(_.addr == host)) {
        log.warning("Got connection message from already connected host {}", host)
      } else {
        //now add it to the list of live hosts
        liveHosts = liveHosts :+ ConnectedHost(host, pool)
      }
    }
  }

  //called in reaction to a pool signalling that it's disconnected
  private def hostDown(host: InetAddress) {
    //sent when a node goes down
    if(!pools.contains(host)) {
      //sanity check - this should not happen, ever
      log.warning("Got down message for unknown host {}", host)
    } else {
      liveHosts = liveHosts.filterNot(_.addr == host)
    }
  }

  //instruct a pool to connect or disconnect
  private def togglePool(host: InetAddress, connect: Boolean = true) {
    pools.get(host).map { pool =>
      if(connect) {
        pool ! Reconnect
      } else {
        pool ! Disconnect
      }
    } getOrElse {
      log.warning("No pool for host {} - cannot process toggle command.", host)
    }
  }

  private def addPool(host: InetAddress) {
    log.debug("Adding pool for {}", host)
    if(pools.contains(host)) {
      throw new IllegalStateException(s"Cannot add pool for already existing host $host")
    }
    val pool = context.watch {
      context.actorOf(
        props = NodeConnector.props(self, new InetSocketAddress(host, setup.port), setup.keyspace, settings.localNodeSettings),
        name = "node-connector-" + actorNameIndex.next() + "-" + host.getHostAddress
      )
    }

    pools = pools.updated(host, pool)
  }

  private def removePool(host: InetAddress) {
    val pool = pools.getOrElse(host, throw new IllegalStateException(s"Cannot find pool for host $host"))

    pools = pools - host
    liveHosts = liveHosts.filterNot(_.addr == host)

    closePool(pool)
  }

  private def closePool(pool: ActorRef): ActorRef = {
    context.unwatch(pool)
    context.actorOf(CloseActor.props(pool, Athena.Close, settings.localNodeSettings.closeTimeout))
  }

  private def mergeMetadata(newMeta: ClusterMetadata) {
    //find the set of newly added hosts
    val currentHosts = clusterMetadata.hosts.keySet
    val newHosts = newMeta.hosts.keySet

    val addedHosts = newHosts.diff(currentHosts)
    val removedHosts = currentHosts.diff(newHosts)

    addedHosts.foreach(addPool)
    removedHosts.foreach(removePool)

    clusterMetadata = newMeta
  }

  private def updateStatus(status: ClusterStatusEvent) {
    currentStatus = status
    statusListeners.foreach(_ ! status)
  }

}

private[athena] object ClusterConnector {

  def props(commander: ActorRef, setup: ClusterConnectorSetup): Props = {
    Props(new ClusterConnector(commander, setup))
  }

  private case class ConnectedHost(addr: InetAddress, connection: ActorRef)

  //sent by various partners of this Actor to indicate that the status of a given Host has changed
  private[connector] case class HostStatusChanged(host: InetAddress, isUp: Boolean)

  private class RequestActor(req: AthenaRequest, respondTo: ActorRef, plan: Iterator[ConnectedHost], timeout: Duration) extends Actor with ActorLogging {

    //
    // TODO - add retry policy logic
    //
    context.setReceiveTimeout(timeout)

    var errors: Map[InetAddress, Any] = Map.empty

    override def preStart() {
      attemptRequest()
    }

    def attemptRequest() {
      if(!plan.hasNext) {
        //out of options here,
        respondTo ! ConnectionUnavailable(req, errors)
        context.stop(self)
      } else {
        val host = plan.next()
        log.debug("Using host {} for request.", host)
        host.connection ! req

        context.become {
          case resp: AthenaResponse =>
            if(resp.isFailure) {
              errors = errors.updated(host.addr, resp)
              attemptRequest()              
            } else {
              respondTo ! resp
              context.stop(self)              
            }

          case ReceiveTimeout =>
            log.warning("Request timed out.")
            errors = errors.updated(host.addr, Timedout(req))
            attemptRequest()

          case x =>
            log.error("Received unknown response to request - {}", x)
            errors = errors.updated(host.addr, x)
            attemptRequest()
        }

      }
    }

    def receive: Receive = {
      case x =>
        log.error("Should not happen - received message {}.")
        respondTo ! RequestFailed(req)
        context.stop(self)
    }
  }

  private object RequestActor {
    def props(req: AthenaRequest, respondTo: ActorRef, plan: Iterator[ConnectedHost], timeout: Duration) =
      Props(new RequestActor(req, respondTo, plan, timeout))
  }

  // TODO: Add more complex (e.g. by token) routing plans
  private class RoutingPlan {

    //hold our state internally - this isn't a big deal because of the fact that this class
    //will only ever be accessed by the actor

    var index = 0

    def generatePlan(req: AthenaRequest, state: IndexedSeq[ConnectedHost])(implicit log: LoggingAdapter): Iterator[ConnectedHost] = {

      val startIndex = index

      //increment the index counter - the next call to this function will start at the next
      //spot in the hosts array
      index = if(index == Int.MaxValue) {
        //just being cautious
        0
      } else {
        index + 1
      }

      val hostsSize = state.size

      new Iterator[ConnectedHost] {
        var currentIndex = startIndex
        var hostsRemaining = hostsSize
        var nextValue = computeNext()

        def next(): ConnectedHost = {
          val current = nextValue
          nextValue = computeNext()
          current.getOrElse(Iterator.empty.next())
        }

        def hasNext: Boolean = nextValue.isDefined

        private def computeNext(): Option[ConnectedHost] = {
          if(hostsRemaining == 0) {
            None
          } else {
            val valueIndex = currentIndex % hostsSize
            currentIndex = currentIndex + 1
            hostsRemaining = hostsRemaining - 1
            Some(state(valueIndex))
          }
        }
      }
    }

  }

}
