package athena.connector

import akka.actor._

import athena.Athena._
import athena.{ConnectionSettings, NodeConnectorSettings, Responses, Athena}
import athena.Requests.{KeyspaceAwareRequest, AthenaRequest}
import athena.Responses.AthenaResponse

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.util.{Timeout, ByteString}
import athena.Athena.NodeDisconnected
import athena.Athena.NodeConnected
import athena.Responses.ConnectionUnavailable
import athena.data.PreparedStatementDef
import akka.actor.Terminated
import athena.connector.ConnectionActor.{ConnectionCommandFailed, KeyspaceChanged, SetKeyspace}
import scala.util.control.NonFatal
import akka.event.LoggingReceive
import athena.connector.ClusterConnector.CheckKeyspace

/**
 * Manages a pool of connections to a single Cassandra node. This Actor takes incoming requests and dispatches
 * them to child managed connections, while at the same time monitoring the state of those connections.
 *
 * @author David Pratt (dpratt@vast.com)
 */
private[athena] class NodeConnector(commander: ActorRef,
                                    remoteAddress: InetSocketAddress,
                                    settings: NodeConnectorSettings,
                                    preparedStatementDefs: Map[ByteString, PreparedStatementDef]) extends Actor with Stash with ActorLogging {

  //
  // TODO - reap connections below the usage threshold
  //

  import NodeConnector._

  import context.dispatcher

  private[this] val counter = Iterator from 0

  //private[this] var livePreparedStatements: Map[ByteString, PreparedStatementDef] = preparedStatementDefs

  //A map holding all requests pending until the availability of a connection to the specified keyspace.
  private[this] var pendingRequests: Map[Option[String], Seq[PendingRequest]] = Map.empty

  //A map of all of our current live connections
  private[this] var liveConnections: Map[ActorRef, ConnectionStatus] = Map.empty

  //Any children we create that signal errors should not be restarted.
  override val supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  private[this] implicit val defaultTimeout = if(settings.connectionSettings.requestTimeout.isFinite()) {
    Timeout(settings.connectionSettings.requestTimeout.length, settings.connectionSettings.requestTimeout.unit)
  } else {
    //just use a default timeout of 10 seconds
    Timeout(10, TimeUnit.SECONDS)
  }

  private val defaultBehavior: Receive = {
    case req: AthenaRequest =>
      sender ! ConnectionUnavailable(req)
      
    case close: Athena.CloseCommand =>
      log.warning("Unhandled close request to node connector. Shutting down.")
      sender ! close.event
      context.stop(self)

    case Terminated(`commander`) =>
      log.error("Node connector commander unexpectedly shut down. Terminating.")
      context.stop(self)

  }

  //death pact with our commander
  context.watch(commander)

  def receive: Receive = connecting()

  //if retryCount is non-negative, we are attempting to reconnect after a disconnection.
  //this method attempts to connect to just a single host - if it's successful, we then open
  //the rest of the connections
  private def connecting(retryCount: Int = -1): Receive = {

    spawnNewConnection(None)

    val behavior: Receive = {
      case ConnectionInitialized(connection, keyspace) =>
        markConnected(connection, keyspace)
        if(retryCount > -1) {
          log.info("Reconnected to host {}", remoteAddress.getAddress)
        } else {
          log.info("Connected to host {}", remoteAddress.getAddress)
        }
        connected()

      case ConnectionAttemptFailed(_, error) =>
        log.warning("Connection to {} failed due to error {}. Reconnecting.", remoteAddress, error)
        reconnecting(retryCount + 1)

      case cmd: Athena.CloseCommand =>
        close(cmd, Set(sender()))
    }

    behavior orElse defaultBehavior
  }

  private def disconnected() = {

    context.setReceiveTimeout(Duration.Inf)

    shutdownAll(Athena.Close)

    //notify the cluster that we've disconnected
    commander ! NodeDisconnected(remoteAddress.getAddress)

    //schedule a reconnect attempt
    reconnecting()
  }

  private def reconnecting(retryCount: Int = 0) {

    context.setReceiveTimeout(Duration.Inf)

    val delay = reconnectDelay(retryCount)
    log.warning("Host {} is unreachable. Scheduling reconnection attempt in {}", remoteAddress.getAddress, delay)
    val reconnectJob = context.system.scheduler.scheduleOnce(delay) {
      log.debug("Attempting reconnection to {}", remoteAddress.getAddress)
      self ! InternalReconnect
    }
    
    val behavior: Receive = {
      case Reconnect =>
        reconnectJob.cancel()
        context.become(connecting(0))

      case InternalReconnect =>
        context.become(connecting(retryCount))
    }

    context.become(behavior orElse defaultBehavior)
  }

  private def connected() {

    context.setReceiveTimeout(Duration.Inf)

    //notify the cluster that we've connected
    commander ! NodeConnected(remoteAddress.getAddress)

    //spawn the rest of the connections in the pool
    // note - we spawn them without a keyspace initially
    (1 until settings.poolSettings.coreConnections).foreach(_ => spawnNewConnection(None))

    def disconnect() = {
      log.debug("Disconnecting pool from host {}", remoteAddress.getAddress)
      liveConnections.keys.foreach(shutdownConnection(_, Athena.Close))
      liveConnections = Map.empty
      disconnected()
    }

    val behavior: Receive = {
      case ConnectionInitialized(connection, keyspace) =>
        log.debug("Host {} adding connection {} to pool", remoteAddress.getAddress, connection)
        markConnected(connection, keyspace)

      case ConnectionAttemptFailed(keyspace, errorOpt) =>
        log.warning("Connection to {} failed. Disconnecting from host.", remoteAddress)
        disconnect()

      case KeyspaceSet(connection, newKeyspace) =>
        markKeyspaceSwitch(connection, newKeyspace)

      case KeyspaceAttemptFailed(newKeyspace, error) =>
        markKeyspaceError(newKeyspace, error)

      case Terminated(child) if liveConnections.contains(child) =>
        log.warning("Connection unexpectedly terminated. Disconnecting node.")
        removeConnection(child)
        disconnect()

      case req: AthenaRequest =>
        dispatch(PendingRequest(req, sender()))

      case Disconnect =>
        disconnect()

      case RequestCompleted(connection) ⇒
        log.debug("Request completed.")
        decrementConnection(connection)

      case Terminated(`commander`) =>
        log.error("Node connector commander unexpectedly shut down. Terminating.")
        close(Athena.Abort, Set())

      case cmd: Athena.CloseCommand =>
        close(cmd, Set(sender()))
    }

    log.debug("Returning connected behavior.")
    context.become(LoggingReceive(behavior orElse defaultBehavior))
  }

  private def close(command: Athena.CloseCommand, commanders: Set[ActorRef]) {

    context.setReceiveTimeout(Duration.Inf)

    log.debug("Closing node connector with active connections - {}", liveConnections.keys)

    def closing(closeActors: Set[ActorRef], command: Athena.CloseCommand, commanders: Set[ActorRef]) {
      log.debug("Moving to closing state with {} live connections.", closeActors.size)

      context.setReceiveTimeout(settings.poolSettings.connectionTimeout)

      def signalClosed() {
        commanders.foreach(_ ! command.event)
        context.stop(self)
      }

      val behavior: Receive = if(closeActors.isEmpty) {
        signalClosed()
        defaultBehavior
      } else {
        {
          case req: AthenaRequest =>
            log.warning("Rejecting request because connector is shutting down.")
            sender ! Responses.ConnectionUnavailable(req)

          case cmd: Athena.CloseCommand =>
            log.debug("Ignoring close command {} - already shutting down.", cmd)
            closing(closeActors, command, commanders + sender)

          case ConnectionInitialized(connection, _) =>
            closing(closeActors + shutdownConnection(connection, command), command, commanders)

          case ConnectionAttemptFailed =>
            //ignore

          case Terminated(child) if closeActors.contains(child) ⇒
            val stillOpen = closeActors - child
            if (stillOpen.isEmpty) {
              signalClosed()
            } else closing(stillOpen, command, commanders)

          case ReceiveTimeout ⇒
            log.warning("Initiating forced shutdown due to close timeout expiring.")
            signalClosed()

          case _: RequestCompleted  ⇒ // ignore
        }
      }

      context.become(behavior orElse defaultBehavior)
    }

    log.debug("Killing all active connections.")
    val closeActors = shutdownAll(command).map(context.watch)

    closing(closeActors, command, commanders)
  }

  private def dispatch(pendingRequest: PendingRequest) {

    val keyspace: Option[String] = pendingRequest.request match {
      case x: KeyspaceAwareRequest => x.keyspace
      case _ => None
    }

    val activeConnections = liveConnections.collect {
      case (connection, x: Active) => (connection, x)
    }

    def attemptLiveConnection(): Boolean = {

      def compatibleConnection(active: (ActorRef, Active)): Boolean = {
        if(active._2.openRequestCount <= settings.poolSettings.maxConcurrentRequests) {
          if(keyspace.isEmpty) {
            //if the statement doesn't have a keyspace, any connection will do
            true
          } else {
            val ks = keyspace.get
            //the keyspace of the connection must be compatible with that of the statement
            active._2.keyspace.exists(_ == ks)
          }
        } else {
          //connection is saturated
          false
        }
      }

      //first, filter out all connections that are not active and incompatible
      //then find the connection with the least amount of active requests
      //if it exists, use it
      val leastBusy = activeConnections.view.filter(compatibleConnection).reduceLeftOption[(ActorRef, Active)] {
        case (acc, active) => if(active._2.openRequestCount < acc._2.openRequestCount) active else acc
      }

      leastBusy.fold(false) {
        case (connection, _) =>
          //fire off a request actor
          context.actorOf(
            props = RequestActor.props(pendingRequest.request, connection, pendingRequest.respondTo, settings.connectionSettings.requestTimeout),
            name = "request-actor-" + counter.next()
          )

          incrementConnection(connection)
          true
      }
    }

    def attemptPendingConnection(): Boolean = {
      //if there is a pending connection attempt for the target keyspace, queue up the request for when it finishes
      val isPending = liveConnections.exists {
        case (_, Connecting(`keyspace`)) => true
        case _ => false
      }

      if(isPending) {
        val existingPending = pendingRequests.getOrElse(keyspace, Seq.empty)
        pendingRequests = pendingRequests.updated(keyspace, pendingRequest +: existingPending)
      }
      isPending
    }

    def attemptSwitch(): Boolean = {
      //if there's no explicit keyspace on the request, which means there's no way to switch
      //an idle connection over to the desired keyspace.
      keyspace.fold(false) { targetKeyspace =>
        //is there already a request to switch to this keyspace for a connection?
        val liveSwitchRequest = liveConnections.exists {
          case (_, SwitchingKeyspace(`targetKeyspace`, _)) => true
          case _ => false
        }

        val ableToSwitch = if(liveSwitchRequest) liveSwitchRequest else {
          //check to see if we can find an idle connection to switch the keyspace on
          val switchCandidate = activeConnections.collectFirst {
            case (connection, active) if active.openRequestCount == 0 => connection
          }
          switchCandidate.fold(false) { toBeSwitched =>
            switchKeyspace(toBeSwitched, targetKeyspace)
            true
          }
        }

        if(ableToSwitch) {
          //don't need to find a connection to switch - just mark the request as pending for the switch result
          val existingPending = pendingRequests.getOrElse(Some(targetKeyspace), Seq.empty)
          pendingRequests = pendingRequests.updated(Some(targetKeyspace), pendingRequest +: existingPending)
        }
        ableToSwitch
      }
    }

    def attemptNewConnection(): Boolean = {
      //find all connections with a keyspace explicitly equal to the requested statement (not just compatible)
      val openInKeyspace = activeConnections.count(x => x._2.keyspace == keyspace)
      if(openInKeyspace < settings.poolSettings.maxConnections) {
        //we can create a new connection - we have not reached the maximum number of simultaneous connections to a keyspace
        spawnNewConnection(keyspace, Some(pendingRequest))
        true
      } else {
        false
      }
    }

    //TODO: This short-circuit/chaning construct is kind of ugly
    if(!attemptLiveConnection()) {
      if(!attemptPendingConnection()) {
        if(!attemptSwitch()) {
          if(!attemptNewConnection()) {
            pendingRequest.respondTo ! ConnectionUnavailable(pendingRequest.request)
          }
        }
      }
    }

  }

  private def incrementConnection(connection: ActorRef) {
    val connState = liveConnections.getOrElse(connection, throw new IllegalStateException("Cannot find entry for connection."))
    val newState = connState match {
      case x: Active => x.copy(openRequestCount = x.openRequestCount + 1)
      case _ =>
        throw new IllegalStateException("Cannot increment connection request count - connection not marked as active.")
    }
    liveConnections = liveConnections.updated(connection, newState)

  }

  private def decrementConnection(connection: ActorRef) {
    val connState = liveConnections.getOrElse(connection, throw new IllegalStateException("Cannot find entry for connection."))
    val newState = connState match {
      case x: Active =>
        if(x.openRequestCount <= 0) {
          throw new IllegalStateException("Cannot decrement connection request count - new count would be negative.")
        } else {
          x.copy(openRequestCount = x.openRequestCount - 1)
        }
      case _ =>
        throw new IllegalStateException("Cannot decrement connection request count - connection not marked as active.")
    }
    liveConnections = liveConnections.updated(connection, newState)
  }

  private def switchKeyspace(connection: ActorRef, keyspace: String) {

    liveConnections.get(connection).fold[Unit](throw new IllegalArgumentException("Cannot switch keyspace for unknown connection.")) {
      case Active(currentKeyspace, 0) =>
        liveConnections = liveConnections.updated(connection, SwitchingKeyspace(keyspace, currentKeyspace))
        import akka.pattern._

        connection.ask(SetKeyspace(keyspace)).map {
          case KeyspaceChanged(newKs) if newKs.toUpperCase == keyspace.toUpperCase =>
            KeyspaceSet(connection, keyspace)

          case KeyspaceChanged(unknown) =>
            log.error("Got a set keyspace response to an incorrect keyspace - {}", unknown)
            KeyspaceAttemptFailed(keyspace, Athena.InternalError("Keyspace operation failed."))

          case ConnectionCommandFailed(_, Some(error)) =>
            log.error("Could not set keyspace {} due to error - {}", keyspace, error)
            KeyspaceAttemptFailed(keyspace, error)

          case ConnectionCommandFailed(_, None) =>
            log.error("Could not set keyspace {}", keyspace)
            KeyspaceAttemptFailed(keyspace, Athena.InternalError("Keyspace operation failed."))

          case keyspaceResponse =>
            log.error("Unexpected response to set keyspace call - {}", keyspaceResponse)
            KeyspaceAttemptFailed(keyspace, Athena.InternalError("Keyspace operation failed."))

        } recover {
          case NonFatal(t) =>
            log.error("Could not set keyspace due to failure - {}", t)
            KeyspaceAttemptFailed(keyspace, Athena.GeneralError("Keyspace operation failed."))
        } pipeTo self

      case Active(_, currentCount) =>
        log.error("Cannot switch keyspace on connection with inflight requests. Current count - {}", currentCount)
        throw new IllegalArgumentException("Cannot switch keyspace on connection with inflight requests.")

      case x: SwitchingKeyspace if x.destinationKeyspace == keyspace =>
        //do nothing - we're already switching it
        log.debug("Doing nothing - already switching keyspace to {}", keyspace)

      case x =>
        log.error("Invalid keyspace request.")
        throw new IllegalArgumentException(s"Request to set keyspace on connection in an invalid state - $x")

    }

  }

  private def addPending(destinationKeyspace: Option[String], request: PendingRequest) {
    val existing = pendingRequests.getOrElse(destinationKeyspace, Seq.empty)
    pendingRequests = pendingRequests.updated(destinationKeyspace, request +: existing)
  }

  private def removePending(destinationKeyspace: Option[String]): Seq[PendingRequest] = {
    val pending = pendingRequests.get(destinationKeyspace).getOrElse(Seq.empty)
    pendingRequests = pendingRequests - destinationKeyspace
    pending
  }

  private def shutdownConnection(connection: ActorRef, cmd: Athena.CloseCommand = Athena.Close): ActorRef = {
    removeConnection(connection)
    context.actorOf(CloseActor.props(connection, cmd, settings.connectionSettings.socketSettings.connectTimeout))
  }

  private def spawnNewConnection(keyspace: Option[String], pendingRequest: Option[PendingRequest] = None) {
    log.debug("Spawning new connection to keyspace {}", keyspace)

    //spawn a connection
    val initializer = context.actorOf(
      props = Props(new ConnectionInitializer(remoteAddress, settings.connectionSettings, keyspace))
    )
    val connection = context.actorOf(ConnectionActor.props(initializer, remoteAddress, settings.connectionSettings, keyspace))

    liveConnections = liveConnections.updated(connection, Connecting(keyspace))

    pendingRequest.foreach(p => addPending(keyspace, p))
  }

  private def markConnected(connection: ActorRef, keyspace: Option[String]) {
    val currentState = liveConnections.getOrElse(connection, throw new IllegalArgumentException("Cannot mark an unknown connection as live."))
    val nextState = currentState match {
      case Connecting(ks) if keyspace == ks => Active(keyspace, 0)
      case Connecting(ks) =>
        log.error("Cannot mark a connection live on keyspace {} - was expecting keyspace {}", keyspace, ks)
        throw new IllegalArgumentException("Cannot mark connection in non-connecting state as live.")
      case x =>
        log.error("Got unexpected state {} for newly-connected connection.", x)
        throw new IllegalArgumentException("Cannot mark connection in non-connecting state as live.")
    }
    liveConnections = liveConnections.updated(connection, nextState)

    //now dispatch any pending requests for a connection to this keyspace
    removePending(keyspace).foreach(dispatch)
  }

  private def markKeyspaceSwitch(connection: ActorRef, newKeyspace: String) {
    val currentState = liveConnections.getOrElse(connection, throw new IllegalArgumentException("Cannot mark an unknown connection with a new keyspace."))
    val nextState = currentState match {
      case SwitchingKeyspace(ks, _) if newKeyspace == ks => Active(Some(newKeyspace), 0)
      case SwitchingKeyspace(ks, _) =>
        log.error("Cannot set the keyspace on connection to {} - was expecting keyspace {}", newKeyspace, ks)
        throw new IllegalArgumentException("Cannot update keyspace.")
      case x =>
        log.error("Got unexpected state {} for connection switching keyspace.", x)
        throw new IllegalArgumentException("Cannot update keyspace.")
    }
    liveConnections = liveConnections.updated(connection, nextState)

    //now dispatch any pending requests for a connection to this keyspace
    removePending(Some(newKeyspace)).foreach(dispatch)
  }

  private def markKeyspaceError(keyspace: String, error: Athena.Error) {
    liveConnections = liveConnections.mapValues {
      case SwitchingKeyspace(destinationKeyspace, previousKeyspace) if keyspace == destinationKeyspace =>
        Active(previousKeyspace, 0)
      case x => x
    }

    //bounce anybody waiting on that keyspace
    //now dispatch any pending requests for a connection to this keyspace
    removePending(Some(keyspace)).foreach(x => x.respondTo ! Responses.ErrorResponse(x.request, error))
  }

  private def removeConnection(connection: ActorRef) {
    context.unwatch(connection)
    liveConnections = liveConnections - connection
  }

  private def shutdownAll(command: Athena.CloseCommand = Athena.Close) : Set[ActorRef] = {
    //bounce all pending requests
    pendingRequests.values.flatten.foreach(x => x.respondTo ! ConnectionUnavailable(x.request))
    //close all existing connections
    liveConnections.keySet.map(shutdownConnection(_, command))
  }

}

private[athena] object NodeConnector {


  private[connector] def props(commander: ActorRef,
            remoteAddress: InetSocketAddress,
            settings: NodeConnectorSettings,
            preparedStatementDefs: Map[ByteString, PreparedStatementDef] = Map.empty): Props = {
    Props(new NodeConnector(commander, remoteAddress, settings, preparedStatementDefs))
  }

  def props(commander: ActorRef, setup: NodeConnectorSetup): Props = props(commander, setup.remoteAddress, setup.settings.get)

  //used internally - this recconnect message won't reset the retry count
  private case object InternalReconnect
  
  //internal messaging events  
  private case class RequestCompleted(connection: ActorRef)

  private case class PendingRequest(request: AthenaRequest, respondTo: ActorRef)

  sealed trait KeyspaceResponse
  private case class KeyspaceSet(connection: ActorRef, newKeyspace: String) extends KeyspaceResponse
  private case class KeyspaceAttemptFailed(keyspace: String, error: Athena.Error) extends KeyspaceResponse

  sealed trait ConnectionResponse
  private case class ConnectionInitialized(connection: ActorRef, keyspace: Option[String]) extends ConnectionResponse
  private case class ConnectionAttemptFailed(keyspace: Option[String], error: Option[Athena.Error]) extends ConnectionResponse

  private sealed trait ConnectionStatus
  private case class Connecting(keyspace: Option[String] = None) extends ConnectionStatus
  private case class Active(keyspace: Option[String] = None, openRequestCount: Int = 0) extends ConnectionStatus
  private case class SwitchingKeyspace(destinationKeyspace: String, previousKeyspace: Option[String]) extends ConnectionStatus

  private case object ReaperTick


  //reconnect policy
  private val BaseRecoDelay: Long = 1000 //1 second
  private val MaxRecoDelay: Long = 10 * 60 * 1000 //10 minutes
  private def reconnectDelay(retryCount: Int = 0): FiniteDuration = {
    //this calculates an exponential reconnection delay dropoff
    Duration(math.min(BaseRecoDelay * (1L << retryCount), MaxRecoDelay), TimeUnit.MILLISECONDS)
    //Duration(1, TimeUnit.MINUTES)
  }

  private class ConnectionInitializer(remoteAddress: InetSocketAddress, settings: ConnectionSettings, keyspace: Option[String]) extends Actor with ActorLogging {

    context.setReceiveTimeout(settings.requestTimeout)

    override def receive: Actor.Receive = {

      case Athena.ConnectionFailed(addr, error) =>
        log.error("Could not connect to {} - {}", addr, error)
        context.parent ! ConnectionAttemptFailed(keyspace, error)
        context.stop(self)

      case Athena.Connected(addr, localAddr) =>
        val connection = sender()
        connection ! Athena.Register(context.parent)
        context.parent ! ConnectionInitialized(connection, keyspace)
        context.stop(self)

      case ReceiveTimeout =>
        log.error("Timed out waiting for connection.")
        context.parent ! ConnectionAttemptFailed(keyspace, None)
        context.stop(self)
    }
  }

  private class RequestActor(req: AthenaRequest, connection: ActorRef, respondTo: ActorRef, requestTimeout: Duration) extends Actor with ActorLogging {

    //request actors should not restart - they are fire and forget, one and done
    override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

    override def preStart(): Unit = {
      context.watch(connection)
      context.setReceiveTimeout(requestTimeout)
      connection ! req
    }

    def sendResponse(r: AthenaResponse) {
      respondTo.tell(r, context.parent)
    }

    def receive: Actor.Receive = {
      case resp: AthenaResponse =>
        //log.debug("Delivering {} for {}", resp, req)
        sendResponse(resp)

        //notify the parent if the request failed - they should close the connection
        if(resp.isFailure) {
          notifyParent()
        }
        context.stop(self)

      case Terminated(`connection`) ⇒
        sendResponse(Responses.RequestFailed(req))
        context.stop(self)

      case ReceiveTimeout =>
        sendResponse(Responses.Timedout(req))
        notifyParent()
        context.stop(self)
    }

    override def postStop(): Unit = {
      context.parent ! RequestCompleted(connection)
    }

    private def notifyParent() {
      log.debug("Sending {} failed, notifying connection holder.", req)
      context.parent ! Disconnect
    }

  }

  private object RequestActor {
    def props(req: AthenaRequest, connection: ActorRef, respondTo: ActorRef, requestTimeout: Duration): Props = {
      Props[RequestActor](new RequestActor(req, connection, respondTo, requestTimeout))
    }
  }
}
