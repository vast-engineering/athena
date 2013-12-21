package athena.connector

import athena.{ClusterConnectorSettings, Athena}
import akka.actor._
import akka.pattern._
import athena.Requests.AthenaRequest
import athena.connector.ClusterInfo.{ClusterMetadata, Host}
import athena.Responses.{Timedout, RequestFailed, AthenaResponse}
import athena.connector.NodeConnector.{NodeConnected, NodeUnavailable}
import scala.concurrent.duration.{FiniteDuration, Duration}
import akka.actor.Status.Failure
import athena.Athena.{ClusterConnectorSetup, NoHostAvailableException}
import java.net.{InetSocketAddress, InetAddress}
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

private[athena] class ClusterConnector(initialHosts: Set[InetAddress],
                                       port: Int,
                                       keyspace: Option[String],
                                       settings: ClusterConnectorSettings) extends Actor with ActorLogging with Stash {

  import ClusterConnector._

  import context.dispatcher

  //sign a death pact with the monitor - we need it to operate
  private[this] val monitorActor = context.watch {
    context.actorOf(
      props = Props(new ClusterMonitorActor(self, initialHosts, port, settings)),
      name = "cluster-monitor"
    )
  }

  private[this] val defaultRequestTimeout = settings.localNodeSettings.connectionSettings.requestTimeout

  private[this] val routingPlan = new RoutingPlan()

  private var clusterMetadata: ClusterMetadata = ClusterMetadata(None, None, Map.empty)
  
  private var disconnectedHosts: Map[InetAddress, DisconnectedHost] = Map.empty
  private var connectingHosts: Map[InetAddress, ConnectingHost] = Map.empty
  private var liveHosts: IndexedSeq[ConnectedHost] = IndexedSeq.empty

  private[this] val actorNameIndex = Iterator from 0

  def receive = starting

  private def starting: Receive = {

    val behavior: Receive = {
      case meta: ClusterMetadata =>
        //create our intitial host connection pools
        meta.hosts.keySet.foreach(addPool)
        clusterMetadata = meta

        context.become(initializing)

      case req: AthenaRequest =>
        //save the request until we're done starting up
        stash()
    }

    behavior orElse running

  }

  private def initializing: Receive = {

    val behavior: Receive = {
      case NodeConnected(addr) =>
        //wait for at least one node to connect before we route any requests
        markConnected(addr, sender)
        unstashAll()
        context.become(running)

      case req: AthenaRequest =>
        //save the request until we're done starting up
        stash()

    }

    behavior orElse running
  }


  private def running: Receive = {

    case req: AthenaRequest =>
      context.actorOf(
        props = RequestActor.props(req, sender, routingPlan.generatePlan(req, liveHosts), defaultRequestTimeout),
        name = "request-actor-" + actorNameIndex.next()
      )

    case newMeta: ClusterMetadata =>
      //find the set of newly added hosts
      val currentHosts = clusterMetadata.hosts.keySet
      val newHosts = newMeta.hosts.keySet

      val addedHosts = newHosts.diff(currentHosts)
      val removedHosts = currentHosts.diff(newHosts)

      addedHosts.foreach(addPool)
      removedHosts.foreach(removePool)
      
      clusterMetadata = newMeta

    case Terminated(pool) if openConnections.contains(pool) =>
       connectionTerminated(pool)

    case NodeConnected(addr) =>
      markConnected(addr, sender)

    case HostStatusChanged(addr, isUp) =>
      if(isUp) {
        addPool(addr)
      } else {
        log.debug("Closing pool for {}", addr)
        //kill any live connections
        removePool(addr)
        scheduleReconnection(addr, 0)
      }

    case Reconnect(host) =>
      if(liveHosts.exists(_.addr == host) || connectingHosts.contains(host)) {
        log.error("Cannot execute reconnection for connected host {}", host)
      } else {
        disconnectedHosts.get(host).map { dh =>
          openConnection(dh.addr, dh.retryCount)
        } getOrElse {
          log.error("Cannot reconnect to host {} - no entry in table.", host)
        }
      }

    case c: Athena.CloseCommand =>
      context.setReceiveTimeout(defaultRequestTimeout)
      context.become(closing(c, sender, closeAll()))

  }

  private def closing(closeCommand: Athena.CloseCommand, commander: ActorRef, liveConnections: Set[ActorRef]): Receive = {
    case Terminated(pool) =>
      context.become(closing(closeCommand, commander, liveConnections - pool))
    case ReceiveTimeout =>
      log.warning("Timed out waiting for pools to close. Just stopping now.")
      commander ! closeCommand.event
      context.stop(self)
  }

  private def openConnections: Set[ActorRef] = connectingHosts.values.map(_.connection).toSet ++ liveHosts.map(_.connection).toSet

  private def connectionTerminated(pool: ActorRef) {
    log.debug("Pool actor {} terminated.", pool)

    //filter this pool out of any of the live or connecting hosts
    val (liveTerminated, liveKept) = liveHosts.partition(_.connection == pool)
    liveHosts = liveKept
    val (connectingTerminated, connectingKept) = connectingHosts.partition(_._2.connection == pool)
    connectingHosts = connectingKept

    //get a tuple of the host address and retry count (if present)
    val terminatedAddresses = liveTerminated.map(lh => lh.addr -> 0) ++ connectingTerminated.values.map(ch => ch.addr -> ch.retryCount)

    if(terminatedAddresses.size == 0) {
      log.warning("Cannot find pool for terminated connection Actor {}", pool)
    } else {
      val disconnectedHost = terminatedAddresses.head
      if(!terminatedAddresses.tail.isEmpty) {
        log.warning("More than one host entry for host {} - discarding all but first.", disconnectedHost._1)
      }
      scheduleReconnection(disconnectedHost._1, disconnectedHost._2)
    }
  }

  private def markConnected(host: InetAddress, connection: ActorRef) {
    //sent after a node successfully connects
    if(liveHosts.exists(_.addr == host)) {
      log.error("Got connection message from already connected host {}", host)
    } else {
      if(disconnectedHosts.get(host).exists(_.retryCount > 0) || connectingHosts.get(host).exists(_.retryCount > 0)) {
        log.info("Reconnected to host {}", host)
      }
      //filter this host out of any of the disconnected or connecting hosts
      disconnectedHosts.get(host).foreach(_.reconnectJob.cancel())
      disconnectedHosts = disconnectedHosts - host

      connectingHosts = connectingHosts - host

      //now add it to the list of live hosts
      liveHosts = liveHosts :+ ConnectedHost(host, connection)
    }
  }

  private def closeAll(): Set[ActorRef] = {
    val allConnections = liveHosts.map(_.connection).toSet ++ connectingHosts.values.map(_.connection).toSet

    liveHosts = IndexedSeq.empty
    connectingHosts = Map.empty

    disconnectedHosts.values.foreach(_.reconnectJob.cancel())
    disconnectedHosts = Map.empty

    allConnections.foreach(_ ! Athena.Close)
    allConnections
  }

  private def addPool(host: InetAddress) {
    log.debug("Adding pool for {}", host)
    //check to see if it's a disconnected - if it is, cancel it's reconnect call
    //and remove it from the list of disconnected hosts
    val (removed, kept) = disconnectedHosts.partition(_._1 == host)
    removed.values.foreach(h => h.reconnectJob.cancel())
    disconnectedHosts = kept

    //if we have an inflight connection attempt for this host, just use that
    //otherwise open a connection
    if(!connectingHosts.contains(host)) {
      val existingPool = liveHosts.find(_.addr == host)
      if(existingPool.isDefined) {
        log.warning("Not adding pool for already existing host {}", host)
      } else {
        openConnection(host)
      }
    }
  }

  private def removePool(host: InetAddress) {
    //remove any disconnected (and thus waiting for reconnection) hosts
    disconnectedHosts.get(host).foreach(h => h.reconnectJob.cancel())
    disconnectedHosts = disconnectedHosts - host

    //stop any in process connection attempts for this host
    connectingHosts.get(host).foreach(h => context.stop(h.connection))
    connectingHosts = connectingHosts - host

    val (removedLive, keptLive) = liveHosts.partition(_.addr == host)
    removedLive.foreach(h => closePool(h.connection))
    liveHosts = keptLive
  }

  def scheduleReconnection(host: InetAddress, retryCount: Int) {
    if(liveHosts.exists(_.addr == host) || connectingHosts.contains(host)) {
      log.error("Cannot schedule reconnection for connected host {}", host)
    } else {

      //kill any outstanding reconnection requests
      disconnectedHosts.get(host).foreach(_.reconnectJob.cancel())
      disconnectedHosts = disconnectedHosts - host

      val delay = reconnectDelay(retryCount)
      log.info("Host {} is unreachable - scheduling reconnection in {}", host.getCanonicalHostName, delay)
      val recoJob = context.system.scheduler.scheduleOnce(delay) {
        self ! Reconnect(host)
      }
      disconnectedHosts = disconnectedHosts.updated(host, DisconnectedHost(host, retryCount + 1, recoJob))
    }
  }


  private def openConnection(host: InetAddress, retryCount: Int = 0) {
    if(liveHosts.exists(_.addr == host) || connectingHosts.contains(host)) {
      log.error("Connection to host {} already open.")
    } else {
      disconnectedHosts.get(host).foreach(_.reconnectJob.cancel())
      disconnectedHosts = disconnectedHosts - host

      val connection = context.watch {
        context.actorOf(
          props = NodeConnector.props(new InetSocketAddress(host, port), keyspace, settings.localNodeSettings),
          name = "node-connector-" + actorNameIndex.next() + "-" + host.getHostAddress
        )
      }
      connectingHosts = connectingHosts.updated(host, ConnectingHost(host, retryCount, connection))
    }
  }

  private def closePool(pool: ActorRef) = {
    //pools have 5 seconds to close before we hard kill them
    context.unwatch(pool)
    pool.ask(Athena.Close)(Timeout(5, TimeUnit.SECONDS)).onFailure {
      case e: AskTimeoutException =>
        log.debug("Close of pool timed out. Hard stopping actor.")
        context.stop(pool)
      case NonFatal(e) =>
        log.error("Unknown error stopping pool. Hard stopping actor. {}", e)
        context.stop(pool)
    }
  }

}

object ClusterConnector {

  def props(initialHosts: Set[InetAddress],
            port: Int,
            keyspace: Option[String],
            settings: ClusterConnectorSettings): Props = {
    Props(new ClusterConnector(initialHosts, port, keyspace, settings))
  }

  def props(setup: ClusterConnectorSetup): Props = {
    props(setup.initialHosts, setup.port, setup.keyspace, setup.settings.get)
  }

  private case class DisconnectedHost(addr: InetAddress, retryCount: Int, reconnectJob: Cancellable)
  private case class ConnectingHost(addr: InetAddress, retryCount: Int, connection: ActorRef)
  private case class ConnectedHost(addr: InetAddress, connection: ActorRef)

  private case class Reconnect(addr: InetAddress)

  //sent by various partners of this Actor to indicate that the status of a given Host has changed
  case class HostStatusChanged(host: InetAddress, isUp: Boolean)

  class RequestActor(req: AthenaRequest, respondTo: ActorRef, plan: Iterator[ConnectedHost], timeout: Duration) extends Actor with ActorLogging {

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
        respondTo ! Failure(new NoHostAvailableException("No hosts available for query.", errors))
        context.stop(self)
      } else {
        val host = plan.next()
        log.debug("Sending request {} to host {}", req, host)
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

          case x@NodeUnavailable(_) =>
            //try the next node
            errors = errors.updated(host.addr, x)
            attemptRequest()

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

  object RequestActor {
    def props(req: AthenaRequest, respondTo: ActorRef, plan: Iterator[ConnectedHost], timeout: Duration) =
      Props(new RequestActor(req, respondTo, plan, timeout))
  }

  private val BaseRecoDelay: Long = 1000 //1 second
  private val MaxRecoDelay: Long = 10 * 60 * 1000 //10 minutes
  private def reconnectDelay(retryCount: Int = 0): FiniteDuration = {
    //this calculates an exponential reconnection delay dropoff
    Duration(math.min(BaseRecoDelay * (1L << retryCount), MaxRecoDelay), TimeUnit.MILLISECONDS)
  }

  // TODO: Add more complex (e.g. by token) routing plans
  private class RoutingPlan {

    //hold our state internally - this isn't a big deal because of the fact that this class
    //will only ever be accessed by the actor

    var index = 0

    def generatePlan(req: AthenaRequest, state: IndexedSeq[ConnectedHost]): Iterator[ConnectedHost] = {

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

        def next(): ConnectedHost = nextValue.getOrElse(Iterator.empty.next())

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
