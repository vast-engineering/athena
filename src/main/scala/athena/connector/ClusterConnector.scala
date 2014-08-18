package athena.connector

import athena._
import akka.actor._
import athena.Requests.AthenaRequest
import athena.Responses._
import scala.concurrent.duration.{FiniteDuration, Duration}
import athena.Athena._
import java.net.{InetSocketAddress, InetAddress}
import athena.connector.ClusterMonitorActor.{ClusterUnreachable, ClusterReconnected}
import athena.Athena.NodeDisconnected
import athena.Responses.RequestFailed
import athena.Athena.NodeConnected
import athena.Athena.ClusterFailed
import athena.Athena.NodeFailed
import athena.Responses.ConnectionUnavailable
import akka.actor.Terminated
import athena.Responses.Timedout
import athena.connector.ClusterInfo.ClusterMetadata
import athena.data.PreparedStatementDef
import akka.actor.Status.Failure
import athena.util.MD5Hash

class ClusterConnector(initialHosts: Set[InetAddress], port: Int,
                                       settings: ClusterConnectorSettings) extends Actor with ActorLogging {

  import ClusterConnector._

  import context.dispatcher

  //create and sign a death pact with the monitor - we need it to operate
  private[this] val clusterMonitor = context.watch {
    context.actorOf(
      props = Props(new ClusterMonitorActor(self, initialHosts, port, settings)),
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

  private[this] var statusListeners: Set[ActorRef] = Set(context.parent)

  private[this] var currentStatus: ClusterStatusEvent = ClusterDisconnected

  private[this] var preparedStatements: Map[MD5Hash, PreparedStatementDef] = Map.empty

  private[this] val defaultBehavior: Receive = {
    case cmd: Athena.CloseCommand =>
      context.become(closing(cmd, Set(sender())))

    case newMeta: ClusterMetadata =>
      mergeMetadata(newMeta)

    case NodeConnected(address) =>
      //NodeConnected and NodeDisconnected are sent by our pools
      // back to us to let us know their state.
      hostUp(address)

    case x@NodeDisconnected(address) =>
      //let the monitor know - in case it hasn't disconnected yet
      clusterMonitor ! x
      hostDown(address)

    case HostStatusChanged(addr, isUp) =>
      //this is sent from the cluster monitor when the cluster signals a node is available
      togglePool(addr, isUp)

    case ClusterUnreachable =>
      //let our commander know
      pools.values.foreach(_ ! Disconnect)
      updateStatus(ClusterDisconnected)

    case ClusterReconnected =>
      //this is sent by the cluster monitor after it has been disconnected from every host in the cluster
      //and has subsequently reconnected - we should tell all our pools to immediately attempt a reconnect
      pools.values.foreach(_ ! Reconnect)
      updateStatus(ClusterConnected)

    case Terminated(listener) if statusListeners.contains(listener) =>
      context.unwatch(listener)
      statusListeners = statusListeners - listener
      //TODO: Stop self upon termination of our final listener

    case AddClusterStatusListener(listener) =>
      listener ! currentStatus
      context.watch(listener)
      statusListeners = statusListeners + listener

    case RemoveClusterStatusListener(listener) =>
      context.unwatch(listener)
      statusListeners = statusListeners - listener

    case x: StatementPrepared =>
      preparedStatements = preparedStatements.updated(x.stmtDef.id, x.stmtDef)
      pools.values.foreach(_ ! x)

  }

  override def postStop(): Unit = {
    log.info("Cluster connector to {} successfully shut down.", clusterMetadata.name.getOrElse("Unknown"))
  }

  def receive = starting()

  private def starting(): Receive = {

    log.debug("Cluster connector starting.")

    val behavior: Receive = {
      case ClusterReconnected =>
        context.become(initializing)

      case req: AthenaRequest =>
        log.warning("Rejecting request while initializing.")
        sender ! ConnectionUnavailable(req)

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
        context.become(closing(Athena.Abort, Set.empty))

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
        val queryPlan = routingPlan.roundRobinPlan(liveHosts)
        context.actorOf(
          props = RequestActor.props(req, sender(), queryPlan, defaultRequestTimeout),
          name = "request-actor-" + actorNameIndex.next()
        )

    }

    behavior orElse defaultBehavior
  }

  private def closing(closeCommand: Athena.CloseCommand, commanders: Set[ActorRef]): Receive = {

    context.setReceiveTimeout(settings.localNodeSettings.closeTimeout)

    log.info("Closing cluster connector {} with {} live pools.", clusterMetadata.name.getOrElse("Unknown"), pools.size)

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

    val toClose = pools.values.toSet + clusterMonitor
    val closeActors = toClose.map { a =>
      context.unwatch(a)
      context.watch(closeActor(a))
    }

    liveHosts = IndexedSeq.empty
    pools = Map.empty

    step(closeActors, commanders ++ statusListeners)
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
    //watch the pool actor, if it dies, so do we.
    val pool = context.watch {
      context.actorOf(
        props = NodeConnector.props(new InetSocketAddress(host, port), settings.localNodeSettings, preparedStatements),
        name = "node-connector-" + actorNameIndex.next()
      )
    }

    pools = pools.updated(host, pool)
  }

  private def removePool(host: InetAddress) {
    val pool = pools.getOrElse(host, throw new IllegalStateException(s"Cannot find pool for host $host"))

    pools = pools - host
    liveHosts = liveHosts.filterNot(_.addr == host)

    closeActor(pool)
  }

  private def closeActor(pool: ActorRef): ActorRef = {
    context.unwatch(pool)
    context.actorOf(CloseActor.props(pool, Athena.Close, settings.localNodeSettings.closeTimeout), name = "close-actor-" + actorNameIndex.next())
  }

  //ingest the new cluster data sent by the monitor actor and adjust our pools accordingly.
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

  //set our status and also let all our listeners know
  private def updateStatus(status: ClusterStatusEvent) {
    currentStatus = status
    statusListeners.foreach(_ ! status)
  }

}

object ClusterConnector {

  def props(initialHosts: Set[InetAddress], port: Int,
            settings: ClusterConnectorSettings): Props = {
    Props(new ClusterConnector(initialHosts, port, settings))
  }

  /**
   * Send to the Cluster actor when connected to test the validity of a keyspace.
   * @param keyspace
   */
  case class CheckKeyspace(keyspace: String)
  case class KeyspaceValid(keyspace: String)

  private case class ConnectedHost(addr: InetAddress, connection: ActorRef)

  //sent by various partners of this Actor to indicate that the status of a given Host has changed
  private[connector] case class HostStatusChanged(host: InetAddress, isUp: Boolean)

  private class RequestActor(originalRequest: AthenaRequest, respondTo: ActorRef, plan: Iterator[ConnectedHost], timeout: Duration) extends Actor with ActorLogging {

    import RequestActor._
    import context.dispatcher

    var errors: Map[InetAddress, AthenaResponse] = Map.empty

    val timeoutJob = timeout match {
      case t: FiniteDuration =>
        val j = context.system.scheduler.scheduleOnce(t) {
          self ! QueryTimeoutExceeded
        }
        Some(j)
      case _ =>
        log.debug("Using infinite timeout for request.")
        None
    }

    override def preStart() {
      attemptRequest(originalRequest)
    }


    override def postStop() {
      timeoutJob.foreach(_.cancel())
    }

    def sendResponse(r: Any) {
      //the response should come from the parent, not us.
      respondTo.tell(r, context.parent)
      context.stop(self)
    }

    //The retry count here is intended to model the number of logical query attempts
    // it shouldn't be incremented due to a transient network error. This is used for things
    // that are cassandra specific. For example, the strategy we use for a read timeout error
    // may depend on the number of attempts. A query sent to a node that is unreachable shouldn't count
    // against this total.
    def attemptRequest(request: AthenaRequest, retryCount: Int = 0) {

      def sendRequest(host: ConnectedHost, retryUnprepared: Boolean = true) {

        host.connection ! request
        context.become {

          case QueryTimeoutExceeded =>
            log.warning("Query timeout exceeded - host: {}", host.addr)
            sendResponse(Timedout(originalRequest))

          case x if sender() != host.connection =>
            log.warning("Received response from an unexpected host. This could be due to a previous timeout.")

          case x@ErrorResponse(_, Errors.OverloadedError(msg, errorHost)) =>
            errors = errors.updated(host.addr, x)
            log.warning("Host is {} overloaded, trying next host. Message - {}", host, msg)
            //note this does not increment the retry count on purpose - it's not an
            // error with the query or state of the cluster itself
            attemptRequest(request, retryCount)

          case ErrorResponse(_, Errors.IsBootstrappingError(msg, errorHost)) =>
            log.error("Query sent to {} but it is bootstrapping. This shouldn't happen but trying next host. Message - {}", errorHost, msg)
            //note this does not increment the retry count on purpose - it's not an
            // error with the query or state of the cluster itself
            attemptRequest(request, retryCount)

          case Responses.ErrorResponse(stmt: Requests.BoundStatement, Errors.UnpreparedError(_, _, _)) if retryUnprepared =>
            //well, this is awkward. Attempt to prepare the statement, then re-execute the query.
            log.debug("Received unprepared error for bound statement. Re-preparing and retrying request. {}", stmt)
            host.connection ! Requests.Prepare(stmt.statementDef.rawQuery, stmt.statementDef.keyspace)
            context.become {
              case Responses.Prepared(_, preparedStmt) =>
                log.debug("Re-prepared statement - {}", preparedStmt.rawQuery)
                sendRequest(host, retryUnprepared = false)

              case resp: AthenaResponse if resp.isFailure =>
                //try the next host
                errors = errors.updated(host.addr, resp)
                attemptRequest(request, retryCount)

              case resp: AthenaResponse if resp.isFailure =>
                sendResponse(Responses.ErrorResponse(request, InternalError(s"Unexpected response to request - $resp")))

              case x: Failure =>
                sendResponse(x)

              case unknown =>
                log.error("Unknown response to request - {}", unknown.toString.take(200))
                sendResponse(Responses.ErrorResponse(request, InternalError(s"Unexpected response to request.")))
            }

          //TODO: handle read and write timeout errors
          // read and write timeouts should increment the retryCount if they fail.


          case resp@Prepared(_, stmtDef) =>
            context.parent ! StatementPrepared(stmtDef)
            sendResponse(resp)

          case resp: AthenaResponse =>
            if(resp.isFailure) {
              //try the next host
              errors = errors.updated(host.addr, resp)
              attemptRequest(request, retryCount)
            } else {
              sendResponse(resp)
            }

          case x: Failure =>
            sendResponse(x)

          case unknown =>
            log.error("Received unknown response - {}", unknown.toString.take(200))
            sendResponse(Responses.ErrorResponse(request, InternalError(s"Unexpected response to request.")))
        }
      }


      if(!plan.hasNext) {
        //out of options here,
        //note - this uses the original request, not the updated one in 'request' that
        // may have been copied and updated by the retry logic.
        sendResponse(NoHostsAvailable(originalRequest, errors))
        context.stop(self)
      } else {
        sendRequest(plan.next())
      }
    }

    def receive: Receive = {
      case x =>
        log.error("Should not happen - received message {}.")
        respondTo ! RequestFailed(originalRequest)
        context.stop(self)
    }
  }

  private object RequestActor {

    private case object QueryTimeoutExceeded

    def props(req: AthenaRequest, respondTo: ActorRef, plan: Iterator[ConnectedHost], timeout: Duration) =
      Props(new RequestActor(req, respondTo, plan, timeout))
  }

  // TODO: Add more complex (e.g. by token) routing plans
  private class RoutingPlan {

    //hold our state internally - this isn't a big deal because of the fact that this class
    //will only ever be accessed by the actor

    var index = 0

    def roundRobinPlan(state: IndexedSeq[ConnectedHost]): Iterator[ConnectedHost] = {

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
