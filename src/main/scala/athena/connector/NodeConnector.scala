package athena.connector

import akka.actor._
import akka.pattern._
import akka.actor.Terminated

import athena._
import athena.Athena._
import athena.Requests._
import athena.Responses._

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.util.Timeout
import scala.util.control.NonFatal
import akka.event.LoggingReceive
import scala.concurrent.{Promise, Future}
import athena.util.MD5Hash
import athena.connector.ConnectionActor.ConnectionCommandFailed
import athena.data.PreparedStatementDef
import athena.connector.ConnectionActor.SetKeyspace
import athena.connector.ConnectionActor.KeyspaceChanged

import scala.collection.mutable

/**
 * Manages a pool of connections to a single Cassandra node. This Actor takes incoming requests and dispatches
 * them to child managed connections, while at the same time monitoring the state of those connections.
 *
 * @author David Pratt (dpratt@vast.com)
 */
private[athena] class NodeConnector(remoteAddress: InetSocketAddress,
                                    settings: NodeConnectorSettings,
                                    preparedStatementDefs: Map[MD5Hash, PreparedStatementDef]) extends Actor with ActorLogging {

  //
  // TODO - reap connections below the usage threshold
  //

  import NodeConnector._

  import context.dispatcher

  private[this] val livePreparedStatements: collection.mutable.Map[MD5Hash, PreparedStatementDef] =
    collection.mutable.Map[MD5Hash, PreparedStatementDef](preparedStatementDefs.toSeq: _*)

  //A map holding all requests pending until the availability of a connection to the specified keyspace.
  private[this] val pendingRequests: collection.mutable.Set[PendingRequest] = mutable.Set.empty

  //A map of all of our current live connections
  private[this] val liveConnections: collection.mutable.Map[ActorRef, ConnectionStatus] = collection.mutable.Map.empty

  //Any children we create that signal errors should not be restarted.
  override val supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  private[this] implicit val defaultTimeout = if(settings.connectionSettings.requestTimeout.isFinite()) {
    Timeout(settings.connectionSettings.requestTimeout.length, settings.connectionSettings.requestTimeout.unit)
  } else {
    //just use a default timeout of 10 seconds
    log.warning("Node connector using default timeout.")
    Timeout(10, TimeUnit.SECONDS)
  }


  override def postStop() {
    //fail any pending requests
    pendingRequests.foreach(r => r.promise.trySuccess(Responses.ConnectionUnavailable(r.request)))
  }

  private val defaultBehavior: Receive = {
    case req: AthenaRequest =>
      sender ! ConnectionUnavailable(req)
      
    case close: Athena.CloseCommand =>
      log.warning("Unhandled close request to node connector. Shutting down.")
      sender ! close.event
      context.stop(self)

    case c: Athena.ConnectionClosed =>
      if(c.isErrorClosed) {
        log.error("Connection closed with error - {}", c.getErrorCause)
      } else {
        log.debug("Connection closed - removing from pool.")
      }
      val connection = sender()
      removeConnection(connection)
      context.stop(connection)
      if(liveConnections.isEmpty) {
        disconnect()
      }

    case Athena.Connected(_, keyspace) =>
      val connection = sender()
      log.warning("Received unexpected connection notification.")
      removeConnection(connection)
      context.stop(connection)

    case Terminated(child) if liveConnections.contains(child) =>
      log.warning("Connection terminated.")
      removeConnection(child)
      if(liveConnections.isEmpty) {
        disconnect()
      }

    case StatementPrepared(statementDef) =>
      livePreparedStatements.update(statementDef.id, statementDef)

    case RemovePending(pending) =>
      pendingRequests.remove(pending)

  }

  def receive: Receive = connecting()

  //if retryCount is non-negative, we are attempting to reconnect after a disconnection.
  //this method attempts to connect to just a single host - if it's successful, we then open
  //the rest of the connections
  private def connecting(retryCount: Int = -1): Receive = {

    if(retryCount > -1) {
      log.warning("Attempting reconnection to {}", remoteAddress.getHostName)
    }
    val connection = spawnNewConnection()
    context.setReceiveTimeout(settings.poolSettings.connectionTimeout)

    val behavior: Receive = LoggingReceive {

      case Athena.Connected(addr, localAddr) if sender() == connection =>
        markConnected(connection, None)
        if(retryCount > -1) {
          log.info("Reconnected to host {}", remoteAddress.getHostName)
        } else {
          log.debug("Connected to host {}", remoteAddress.getHostName)
        }
        preparingStatements()

      case Athena.ConnectionFailed(_, error) if sender() == connection =>
        closeConnection(connection)
        log.warning("Connection to {} failed due to error {}. Scheduling reconnect.", remoteAddress.getHostName, error)
        reconnecting(retryCount + 1)

      case ReceiveTimeout =>
        log.error("Timed out waiting for connection. Retrying.")
        closeConnection(connection)
        reconnecting(retryCount + 1)

      case cmd: Athena.CloseCommand =>
        context.stop(connection)
        close(cmd, Set(sender()))
    }

    behavior orElse defaultBehavior
  }

  private def reconnecting(retryCount: Int = 0) {

    context.setReceiveTimeout(Duration.Inf)

    val delay = reconnectDelay(retryCount)
    log.warning("Host {} is unreachable. Scheduling reconnection attempt in {}", remoteAddress.getHostName, delay)
    val reconnectJob = context.system.scheduler.scheduleOnce(delay) {
      log.debug("Reconnect delay fired - reconnecting to {}", remoteAddress.getHostName)
      self ! InternalReconnect
    }
    
    val behavior: Receive = {
      case Reconnect =>
        log.info("Reconnecting to {}", remoteAddress.getHostName)
        reconnectJob.cancel()
        context.become(connecting(0))

      case InternalReconnect =>
        context.become(connecting(retryCount))
    }

    context.become(behavior orElse defaultBehavior)
  }

  private def disconnect() = {
    log.debug("Disconnecting pool from host {}", remoteAddress.getHostName)
    context.setReceiveTimeout(Duration.Inf)

    shutdownAll(Athena.Close)

    //notify the cluster that we've disconnected
    context.parent ! NodeDisconnected(remoteAddress.getAddress)

    //schedule a reconnect attempt
    reconnecting()
  }

  private[this] val connectedDefault: Receive = ({
    case Athena.Connected(_, keyspace) =>
      val connection = sender()
      log.debug("Host {} adding connection {} to pool", remoteAddress.getHostName, connection)
      markConnected(connection, keyspace)

    case Athena.ConnectionFailed(_, errorOpt) =>
      log.warning("Connection to {} failed. Disconnecting from host.", remoteAddress.getHostName)
      disconnect()

    case KeyspaceSet(connection, newKeyspace) =>
      markKeyspaceSwitch(connection, newKeyspace)

    case KeyspaceAttemptFailed(connection, error) =>
      markKeyspaceError(connection, error)

    case Disconnect =>
      disconnect()

    case RequestCompleted(connection) ⇒
      decrementConnection(connection)

    case StatementPrepared(statementDef) if sender() == self =>
      log.debug("Recording prepared statement.")
      //this means we have prepared a new statement
      livePreparedStatements.update(statementDef.id, statementDef)

    case StatementPrepared(statementDef) if livePreparedStatements.contains(statementDef.id) =>
      //somebody else has prepared a statement - we need to prepare it on our host as well
      log.debug("Got notification to prepare statement.")
      dispatch(Requests.Prepare(statementDef.rawQuery, statementDef.keyspace)).onSuccess {
        case Responses.Prepared(_, stmt) =>
          self ! StatementPrepared(stmt)
        case unknown =>
          log.warning("Unknown response to prepare request - {}", unknown)
      }

    case cmd: Athena.CloseCommand =>
      close(cmd, Set(sender()))

  }: Receive) orElse defaultBehavior


  private def preparingStatements() {
    //spawn the rest of the connections in the pool
    // note - we spawn them without a keyspace initially
    (1 until settings.poolSettings.coreConnections).foreach(_ => spawnNewConnection(None))

    if(livePreparedStatements.isEmpty) {
      running()
    } else {
      //we need to prepare all our statements
      prepareAllStatements(livePreparedStatements.values).map(StatementsPrepared) pipeTo self
      val behavior = ({
        case StatementsPrepared(invalidIds) =>
          if(!invalidIds.isEmpty) {
            log.warning("Discarding invalid prepared statements.")
            invalidIds.foreach(livePreparedStatements.remove)
          }
          running()
      }: Receive) orElse connectedDefault
      context.become(behavior)
    }
  }

  private def running() {

    context.setReceiveTimeout(Duration.Inf)

    //notify the cluster that we've connected
    context.parent ! NodeConnected(remoteAddress.getAddress)

    val behavior: Receive = {
      case prepare: Requests.Prepare =>
        val responseF = dispatch(prepare) pipeTo sender()
        responseF.onSuccess {
          case Responses.Prepared(_, statementDef) =>
            self ! StatementPrepared(statementDef)
          case unknown =>
            log.warning("Unknown response to prepare request - {}", unknown)
        }

      case req: AthenaRequest =>
        dispatch(req) pipeTo sender()

    }

    context.become(behavior orElse connectedDefault)
  }

  private def close(command: Athena.CloseCommand, commanders: Set[ActorRef]) {

    context.setReceiveTimeout(settings.poolSettings.connectionTimeout)

    log.debug("Closing node connector with active connections - {}", liveConnections.keys)

    def closeStep(closeActors: Set[ActorRef], command: Athena.CloseCommand, commanders: Set[ActorRef]) {
      log.debug("Moving to closing state with {} live connections.", closeActors.size)

      def signalClosed() {
        commanders.foreach(_ ! command.event)
        context.stop(self)
      }

      if(closeActors.isEmpty) {
        signalClosed()
      } else {
        context.become {
          case req: AthenaRequest =>
            log.warning("Rejecting request because connector is shutting down.")
            sender ! Responses.ConnectionUnavailable(req)

          case cmd: Athena.CloseCommand =>
            log.debug("Ignoring close command {} - already shutting down.", cmd)
            closeStep(closeActors, command, commanders + sender)

          case Terminated(child) =>
            val stillOpen = closeActors - child
            if (stillOpen.isEmpty) {
              signalClosed()
            } else {
              closeStep(stillOpen, command, commanders)
            }

          case ReceiveTimeout ⇒
            log.warning("Initiating forced shutdown due to close timeout expiring.")
            signalClosed()

          case _: RequestCompleted  ⇒ // ignore
        }
      }
    }

    log.debug("Killing all active connections.")
    val closeActors = shutdownAll(command).map(context.watch)

    closeStep(closeActors, command, commanders)
  }

  private def dispatch(pending: PendingRequest) {
    pending.promise.tryCompleteWith(dispatch(pending.request))
  }

  private def dispatch(request: AthenaRequest): Future[AthenaResponse] = {

    val requestKeyspace: Option[String] = request match {
      case x: KeyspaceAwareRequest => x.keyspace
      case _ => None
    }

    val activeConnections = liveConnections.collect {
      case (connection, x: Active) => (connection, x)
    }

    def attemptLiveConnection(): Option[Future[AthenaResponse]] = {

      def compatibleConnection(active: (ActorRef, Active)): Boolean =
        active._2.openRequestCount <= settings.poolSettings.maxConcurrentRequests && keyspacesCompatible(requestKeyspace, active._2.keyspace)

      //first, filter out all connections that are not active and incompatible
      //then find the connection with the least amount of active requests
      //if it exists, use it
      val leastBusy = activeConnections.view.filter(compatibleConnection).reduceLeftOption[(ActorRef, Active)] {
        case (acc, active) => if(active._2.openRequestCount < acc._2.openRequestCount) active else acc
      }

      leastBusy.map {
        case (connection, _) =>
          sendRequest(request, connection)
      }
    }

    def attemptPending(): Option[Future[AthenaResponse]] = {
      //if one of our connections is on the way to being compatable with this request, queue it up
      val isPending = liveConnections.exists {
        case (_, Connecting(targetKeyspace)) => keyspacesCompatible(requestKeyspace, targetKeyspace)
        case (_, SwitchingKeyspace(targetKeyspace, _)) => keyspacesCompatible(requestKeyspace, Some(targetKeyspace))
        case _ => false
      }

      if(isPending) {
        Some(queueRequest(request, requestKeyspace))
      } else {
        None
      }
    }

    def attemptSwitch(): Option[Future[AthenaResponse]] = {
      //if there's no explicit keyspace on the request, there's no way to switch
      //an idle connection over to the desired keyspace.
      requestKeyspace.flatMap { targetKeyspace =>
        //check to see if we can find an idle connection to switch the keyspace on
        val switchCandidate = activeConnections.collectFirst {
          case (connection, active) if active.openRequestCount == 0 => connection
        }
        switchCandidate.map { toBeSwitched =>
          switchKeyspace(toBeSwitched, targetKeyspace)
          queueRequest(request, requestKeyspace)
        }
      }
    }

    def attemptNewConnection(): Option[Future[AthenaResponse]] = {
      //find all connections with a keyspace explicitly equal to the requested statement (not just compatible)
      val openInKeyspace = activeConnections.count(x => x._2.keyspace == requestKeyspace)
      if(openInKeyspace < settings.poolSettings.maxConnections) {
        //we can create a new connection - we have not reached the maximum number of simultaneous connections to a keyspace
        spawnNewConnection(requestKeyspace)
        Some(queueRequest(request, requestKeyspace))
      } else {
        None
      }
    }

    attemptLiveConnection() orElse
      attemptPending() orElse
      attemptSwitch() orElse
      attemptNewConnection() getOrElse Future.successful(ConnectionUnavailable(request))

  }

  //attempt to re-prepare a batch of statements. This method returns a Future of a collection of IDs
  //for all of the *invalid* statements in the collection
  private def prepareAllStatements(statements: Iterable[PreparedStatementDef]): Future[Iterable[MD5Hash]] = {
    log.debug("Preparing {} statements.", statements.size)
    val futures = statements.map { s =>
      prepareStatement(s) map { prepared =>
        None
      } recover {
        case NonFatal(e) =>
          log.error("Error preparing statement - {}")
          Some(s.id)
      }
    }
    Future.sequence(futures).map(_.flatten)
  }

  //Attempt to prepare a single statement - this method returns a Future containing the statement definition
  // or an exception if there was a problem preparing it on this host.
  private def prepareStatement(statementDef: PreparedStatementDef): Future[PreparedStatementDef] = {
    dispatch(Requests.Prepare(statementDef.rawQuery, statementDef.keyspace)).map {
      case Responses.Prepared(_, stmt) =>
        log.debug("Successfully prepared statement - {}", stmt.rawQuery)
        stmt
      case Responses.ErrorResponse(_, error) =>
        log.error("Error preparing statement - {}", error)
        throw error.toThrowable
      case unknown =>
        log.error("Unexpected response to prepare call - ", unknown)
        throw new InternalException("Unexpected response to prepare call.")
    }
  }

  private def incrementConnection(connection: ActorRef) {
    val connState = liveConnections.getOrElse(connection, throw new IllegalStateException("Cannot find entry for connection."))
    val newState = connState match {
      case x: Active => x.copy(openRequestCount = x.openRequestCount + 1)
      case _ =>
        throw new IllegalStateException("Cannot increment connection request count - connection not marked as active.")
    }
    liveConnections.update(connection, newState)
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
    liveConnections.update(connection, newState)
  }

  private def switchKeyspace(connection: ActorRef, keyspace: String) {

    log.debug("Switching keyspace to {}", keyspace)

    liveConnections.get(connection).fold[Unit](throw new IllegalArgumentException("Cannot switch keyspace for unknown connection.")) {
      case Active(currentKeyspace, 0) =>
        liveConnections.update(connection, SwitchingKeyspace(keyspace, currentKeyspace))
        import akka.pattern._

        connection.ask(SetKeyspace(keyspace)).map {
          case KeyspaceChanged(newKs) if newKs.toUpperCase == keyspace.toUpperCase =>
            KeyspaceSet(connection, keyspace)

          case KeyspaceChanged(unknown) =>
            log.error("Got a set keyspace response to an incorrect keyspace - {}", unknown)
            KeyspaceAttemptFailed(connection, Athena.InternalError("Keyspace operation failed."))

          case ConnectionCommandFailed(_, Some(error)) =>
            log.debug("Could not set keyspace {} due to error - {}", keyspace, error)
            KeyspaceAttemptFailed(connection, error)

          case ConnectionCommandFailed(_, None) =>
            log.debug("Could not set keyspace {}", keyspace)
            KeyspaceAttemptFailed(connection, Athena.InternalError("Keyspace operation failed."))

          case keyspaceResponse =>
            log.error("Unexpected response to set keyspace call - {}", keyspaceResponse)
            KeyspaceAttemptFailed(connection, Athena.InternalError("Keyspace operation failed."))

        } recover {
          case NonFatal(t) =>
            log.debug("Could not set keyspace due to failure - {}", t)
            KeyspaceAttemptFailed(connection, Athena.GeneralError("Keyspace operation failed."))
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

  private val connectionCounter = Iterator from 0

  private def spawnNewConnection(keyspace: Option[String] = None): ActorRef = {
    log.debug("Spawning new connection to keyspace {}", keyspace)
    val connection = context.watch(context.actorOf(ConnectionActor.props(remoteAddress, settings.connectionSettings, keyspace), name = s"connection-${connectionCounter.next()}"))
    liveConnections.update(connection, Connecting(keyspace))
    connection
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
    liveConnections.update(connection, nextState)

    //now dispatch any pending requests for a connection to this keyspace
    dequeuePending(keyspace).foreach(dispatch)
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
    liveConnections.update(connection, nextState)

    //now dispatch any pending requests for a connection to this keyspace
    dequeuePending(Some(newKeyspace)).foreach(dispatch)

  }

  private def markKeyspaceError(connection: ActorRef, error: Athena.Error) {
    val oldState = liveConnections.getOrElse(connection, throw new IllegalArgumentException("Cannot mark keyspace failure on unknown connection."))
    val (newState, failedKeyspace) = oldState match {
      case SwitchingKeyspace(target, previous) => (Active(previous, 0), target)
      case x =>
        log.error("Got unexpected state {} for connection switching keyspace.", x)
        throw new IllegalArgumentException("Cannot update keyspace.")

    }
    liveConnections.update(connection, newState)
    //bounce anybody waiting on that keyspace explicitly - re-dispatch anybody who is waiting for a generic connection
    dequeuePending(Some(failedKeyspace)).foreach { p =>
      if(p.keyspace.isDefined) {
        p.promise.trySuccess(Responses.ErrorResponse(p.request, error))
      } else {
        dispatch(p)
      }
    }
  }

  private def removeConnection(connection: ActorRef) {
    context.unwatch(connection)
    //remove the conenction and possibly bounce anybody waiting on it
    val removed = liveConnections.remove(connection)

    removed.foreach {
        case SwitchingKeyspace(failedKeyspace, _) =>
          //bounce anybody waiting on that keyspace explicitly - re-dispatch anybody who is waiting for a generic connection
          dequeuePending(Some(failedKeyspace)).foreach { p =>
            if(p.keyspace.isDefined) {
              p.promise.trySuccess(Responses.ErrorResponse(p.request, ConnectionError(remoteAddress.getHostName, remoteAddress.getPort)))
            } else {
              dispatch(p)
            }
          }

        case Connecting(keyspaceOpt) =>
          //now bounce any pending requests for a connection to this keyspace
          dequeuePending(keyspaceOpt).foreach { p =>
            p.promise.trySuccess(Responses.ErrorResponse(p.request, ConnectionError(remoteAddress.getHostName, remoteAddress.getPort)))
          }

        case Active(_, _) =>
          //no action - these are live on the connection and the reqeusts will bounce automatically
    }

//      case SwitchingKeyspace(destinationKeyspace, _) =>
//        //if this is the only keyspace switch active for a given keyspace
//    }
  }

  private def dequeuePending(connectionKeyspace: Option[String]): Iterable[PendingRequest] = {
    val dequeued = pendingRequests.filter(p => keyspacesCompatible(p.keyspace, connectionKeyspace))
    pendingRequests --= dequeued
    dequeued
  }


  private[this] val closerCounter = Iterator from 0
  private def shutdownAll(command: Athena.CloseCommand = Athena.Close): Set[ActorRef] = {
    //bounce all pending requests
    pendingRequests.foreach(x => x.promise.trySuccess(ConnectionUnavailable(x.request)))
    pendingRequests.clear()
    //close all existing connections - they have 4 seconds to die
    val closeActors: Set[ActorRef] = liveConnections.keys.map(closeConnection)(collection.breakOut)
    liveConnections.clear()
    closeActors
  }

  private def closeConnection(c: ActorRef): ActorRef = {
    removeConnection(c)
    context.actorOf(CloseActor.props(c, Athena.Close, FiniteDuration(4, TimeUnit.SECONDS)), name = s"pool-closer-${closerCounter.next()}")
  }

  //SHOULD ONLY BE CALLED FROM THIS ACTOR'S RECEIVE LOOP - THIS MODIFIES STATE.
  // Do *NOT* call this from a future callback.
  private def sendRequest(request: AthenaRequest, connection: ActorRef): Future[AthenaResponse] = {

    import akka.pattern._

    incrementConnection(connection)

    val requestF: Future[AthenaResponse] = connection.ask(request)(defaultTimeout).flatMap {

      case resp: AthenaResponse =>
        Future.successful(resp)

      case unknown =>
        log.error("Unknown response to request - {}", unknown.toString().take(200))
        throw new InternalException(s"Unknown response to request.")
    } recover {
      case e: AskTimeoutException =>
        log.error("Request timed out!")
        Timedout(request)
    }

    requestF.onComplete {
      case _ =>
        self ! RequestCompleted(connection)
    }

    //TODO: Should we disconnect if the request fails with an exception?

    requestF
  }


  private def queueRequest(request: AthenaRequest, keyspace: Option[String])(implicit timeout: Timeout): Future[AthenaResponse] = {

    val p = Promise[AthenaResponse]()
    val pending = PendingRequest(request, keyspace, p)
    val timeoutJob = context.system.scheduler.scheduleOnce(timeout.duration) {
      log.debug("Response promise for request timed out - {}", request)
      p.trySuccess(Timedout(request))
    }

    val f = p.future
    f.onComplete {
      case _ =>
        //log.debug("Response completed for queued request - {}", request)
        //cancel the timeout
        timeoutJob.cancel()
        //remove it from the list of pending requests
        self ! RemovePending(pending)
    }

    pendingRequests += pending

    f
  }

}

private[athena] object NodeConnector {


  private[athena] def props(
            remoteAddress: InetSocketAddress,
            settings: NodeConnectorSettings,
            preparedStatementDefs: Map[MD5Hash, PreparedStatementDef] = Map.empty): Props = {
    Props(new NodeConnector(remoteAddress, settings, preparedStatementDefs))
  }

  //used internally - this recconnect message won't reset the retry count
  private case object InternalReconnect
  
  //internal messaging events  
  private case class RequestCompleted(connection: ActorRef)

  private case class PendingRequest(request: AthenaRequest, keyspace: Option[String], promise: Promise[AthenaResponse])
  private case class RemovePending(pending: PendingRequest)

  private sealed trait KeyspaceResponse
  private case class KeyspaceSet(connection: ActorRef, newKeyspace: String) extends KeyspaceResponse
  private case class KeyspaceAttemptFailed(connection: ActorRef, error: Athena.Error) extends KeyspaceResponse

  private sealed trait ConnectionStatus
  private case class Connecting(keyspace: Option[String] = None) extends ConnectionStatus
  private case class Active(keyspace: Option[String] = None, openRequestCount: Int = 0) extends ConnectionStatus
  private case class SwitchingKeyspace(destinationKeyspace: String, previousKeyspace: Option[String]) extends ConnectionStatus

  private case class StatementsPrepared(invalidIds: Iterable[MD5Hash])

  private case object ReaperTick


  //reconnect policy
  private val BaseRecoDelay: Long = 1000 //1 second
  private val MaxRecoDelay: Long = 10 * 60 * 1000 //10 minutes
  private def reconnectDelay(retryCount: Int = 0): FiniteDuration = {
    //this calculates an exponential reconnection delay dropoff
    Duration(math.min(BaseRecoDelay * (1L << retryCount), MaxRecoDelay), TimeUnit.MILLISECONDS)
    //Duration(1, TimeUnit.MINUTES)
  }

  private def keyspacesCompatible(requestKeyspace: Option[String], targetKeyspace: Option[String]): Boolean = {
    //if the statement doesn't have a keyspace, it's compatible with any other keyspace
    requestKeyspace.fold(true) { ks =>
      //if the keyspace is defined, the target must also be defined and be equal to the request's
      targetKeyspace.isDefined && targetKeyspace.get == ks
    }
  }
}
