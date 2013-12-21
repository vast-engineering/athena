package athena.connector

import akka.actor._
import athena.{NodeConnectorSettings, Responses, Athena}
import athena.Requests.AthenaRequest
import scala.concurrent.duration.Duration
import athena.Responses.AthenaResponse
import java.net.{InetAddress, InetSocketAddress}
import athena.Athena.NodeConnectorSetup

/**
 * Manages a pool of connections to a single Cassandra node. This Actor takes incoming requests and dispatches
 * them to child managed connections, while at the same time monitoring the state of those connections.
 *
 * @author David Pratt (dpratt@vast.com)
 */
private[athena] class NodeConnector(remoteAddress: InetSocketAddress,
                                    keyspace: Option[String],
                                    settings: NodeConnectorSettings) extends Actor with ActorLogging {

  //
  // TODO - reap connections below the usage threshold
  //

  import NodeConnector._

  private[this] var connecting = Set.empty[ActorRef]
  private[this] var activeConnections = Map.empty[ActorRef, Int]
  private[this] var idleConnections = Set.empty[ActorRef]
  private[this] val counter = Iterator from 0

  // we cannot sensibly recover from crashes
  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart() {
    for(i <- 0 until settings.poolSettings.coreConnections) {
      spawnNewConnection()
    }
  }

  def receive: Actor.Receive = starting()

  context.setReceiveTimeout(settings.connectionSettings.socketSettings.connectTimeout)

  private def starting(openRequests: Set[(AthenaRequest, ActorRef)] = Set.empty): Receive = {
    case Athena.Connected(remote, local) =>
      addConnection(sender)
      if(activeConnections.size == settings.poolSettings.coreConnections) {
        openRequests.foreach {
          case (req, respondTo) => dispatch(req, respondTo)
        }
        context.setReceiveTimeout(Duration.Inf)
        context.parent ! NodeConnected(remoteAddress.getAddress)
        context.become(running)
      }

    case Athena.CommandFailed(Athena.Connect(_, _, _, _)) =>
      //couldn't connect to the host - this is a fatal signal for us - we need to shut down
      removeConnection(sender)
      close(None, Set())

    case Terminated(child) if childConnections.contains(child) ⇒
      //this means a connection died, and thus we need to die as well
      removeConnection(sender)
      close(None, Set())

    case cmd: Athena.CloseCommand =>
      close(Some(cmd), Set(sender))

    case req: AthenaRequest =>
      //save the request for when we're done creating the core connections.
      context.become(starting(openRequests + (req -> sender)))

    case ReceiveTimeout =>
      log.error("Timed out waiting for core connections.")
      close(None, Set())

  }

  private def running: Receive = {

    case Athena.Connected(remote, local) =>
      addConnection(sender)

    case Athena.CommandFailed(Athena.Connect(remoteHost, _, _, _)) =>
      //couldn't connect to the host - this is a fatal signal for us - we need to shut down
      removeConnection(sender)
      close(None, Set())

    case Terminated(child) if childConnections.contains(child) ⇒
      //this means a connection died, and thus we need to die as well
      //actor is already dead, don't kill it
      removeConnection(sender)
      close(None, Set())

    case req: AthenaRequest =>
      dispatch(req, sender)

    case KillConnection =>
      close(None, Set())

    case RequestCompleted(connection) ⇒
      log.debug("Request completed.")
      decrementConnection(connection)

    case cmd: Athena.CloseCommand =>
      close(Some(cmd), Set(sender))

  }

  private def childConnections = connecting ++ activeConnections.keySet

  private def close(command: Option[Athena.CloseCommand], commanders: Set[ActorRef]) = {

    log.debug("Closing node connector with active connections - {}", activeConnections)

    val closeCommand = command.getOrElse(Athena.Close)

    def signalClosed() {
      commanders.foreach(_ ! closeCommand.event)
      context.stop(self)
    }

    def closing(command: Athena.CloseCommand, connected: Set[ActorRef], commanders: Set[ActorRef]): Receive = {
      log.debug("Moving to closing state with {} live connections - {}", connected.size, connected)

      context.setReceiveTimeout(settings.poolSettings.connectionTimeout)

      {
        case req: AthenaRequest =>
          log.warning("Rejecting request because connector is shutting down.")
          sender ! Responses.RequestFailed(req)

        case cmd: Athena.CloseCommand =>
          log.debug("Got close command {} from {}", cmd, sender)
          context.become(closing(cmd, connected, commanders + sender))

        case Athena.Connected(_, _) =>
          sender ! Athena.Close

        case Athena.CommandFailed(Athena.Connect(remoteHost, _, _, _)) =>
        //ignore

        case Terminated(child) ⇒
          val stillConnected = connected - child
          if (stillConnected.isEmpty) {
            signalClosed()
          } else context.become(closing(command, stillConnected, commanders))

        case ReceiveTimeout ⇒
          log.warning("Initiating forced shutdown due to close timeout expiring.")
          signalClosed()

        case _: RequestCompleted  ⇒ // ignore
      }
    }

    val open = childConnections

    if(open.isEmpty) {
      log.debug("Stopping immediately.")
      signalClosed()
    } else {
      log.debug("Killing all active connections.")
      open.foreach(_ ! closeCommand)
      context.become(closing(closeCommand, open, commanders))
    }
  }

  private def incrementConnection(conn: ActorRef) {
    val countOpt = activeConnections.get(conn)
    if(countOpt.isEmpty) {
      log.warning("Could not find slot for connection. Ignoring.")
    } else {
      val count = countOpt.get
      activeConnections = activeConnections.updated(conn, count + 1)
      idleConnections = idleConnections - conn
    }
  }

  private def decrementConnection(conn: ActorRef) {
    val countOpt = activeConnections.get(conn)
    if(countOpt.isEmpty) {
      log.warning("Could not find slot for connection. Ignoring.")
    } else {
      val newCount = countOpt.get - 1
      if(newCount >= 0) {
        activeConnections = activeConnections.updated(conn, newCount)
      } else {
        log.error("Attempt to set usage count for connection to negative number.")
      }
    }
  }

  private def addConnection(conn: ActorRef) {
    log.debug("New connection opened - {}", conn)
    connecting = connecting - conn
    activeConnections = activeConnections.updated(conn, 0)
    idleConnections = idleConnections + conn
  }

  private def removeConnection(conn: ActorRef) {
    log.debug("Removing connection - {}", conn)
    connecting = connecting - conn
    activeConnections = activeConnections - conn
    idleConnections = idleConnections - conn
  }

  private def firstIdleConnection: Option[ActorRef] = {
    val opt = idleConnections.headOption
    if(log.isDebugEnabled) {
      if(opt.isDefined) {
        log.debug("Using idle connection.")
      } else {
        log.debug("No idle connections.")
      }
    }
    opt
  }

  private def spawnNewConnection() {
    log.debug("Spawning connection.")
    val connect = Athena.Connect(remoteAddress, keyspace, Some(settings.connectionSettings), None)
    val actor = context.watch {
      context.actorOf(
        props = Props(new ConnectionActor(self, connect, settings.connectionSettings)),
        name = "connection-" + counter.next().toString)
    }
    connecting = connecting + actor
  }

  private def pickConnection(): Option[ActorRef] = {
    val connectionOpt = firstIdleConnection orElse {
      //find the least busy active connection and it's current request count
      val leastBusy = activeConnections.reduceLeftOption[(ActorRef, Int)] {
        case (acc, tuple) => if(tuple._2 < acc._2) tuple else acc
      }
      //if the least busy connection has more than the max number of simultaneous requests,
      //and we have fewer than the max number of connections, spawn another one
      leastBusy.foreach {
        case (connection, activeCount) =>
          if(activeCount > settings.poolSettings.maxConcurrentRequests && activeConnections.size < settings.poolSettings.maxConnections) {
            spawnNewConnection()
          }
      }
      leastBusy.map(_._1)

    }
    connectionOpt.foreach(incrementConnection(_))
    connectionOpt
  }

  private def dispatch(req: AthenaRequest, respondTo: ActorRef): Unit = {
    val connectionOpt = pickConnection()
    if(connectionOpt.isEmpty) {
      sender ! NodeUnavailable(req)
    } else {
      context.actorOf(
        props = RequestActor.props(req, connectionOpt.get, respondTo, settings.connectionSettings.requestTimeout),
        name = "request-actor-" + counter.next()
      )
    }
  }

}

private[athena] object NodeConnector {

  def props(remoteAddress: InetSocketAddress,
            keyspace: Option[String],
            settings: NodeConnectorSettings): Props = {
    Props(new NodeConnector(remoteAddress, keyspace, settings))
  }

  def props(setup: NodeConnectorSetup): Props = props(setup.remoteAddress, setup.keyspace, setup.settings.get)

  //sent when the node pool finishes initialization
  case class NodeConnected(addr: InetAddress)

  //sent by this Actor to the original sender when all connections to this host are saturated
  case class NodeUnavailable(request: AthenaRequest)

  case class RequestCompleted(connection: ActorRef)

  case object KillConnection

  private class RequestActor(req: AthenaRequest, connection: ActorRef, respondTo: ActorRef, requestTimeout: Duration) extends Actor with ActorLogging {

    override def preStart(): Unit = {
      context.watch(connection)
      context.setReceiveTimeout(requestTimeout)
      connection ! req
    }

    def receive: Actor.Receive = {
      case resp: AthenaResponse =>
        log.debug("Delivering {} for {}", resp, req)
        respondTo ! resp

        //notify the parent if the request failed - they should close the connection
        if(resp.isFailure) {
          notifyParent()
        }
        context.stop(self)

      case Terminated(`connection`) ⇒
        respondTo ! Responses.RequestFailed(req)
        context.stop(self)

      case ReceiveTimeout =>
        respondTo ! Responses.Timedout(req)
        notifyParent()
        context.stop(self)
    }

    override def postStop(): Unit = {
      context.parent ! RequestCompleted(connection)
    }

    private def notifyParent() {
      log.debug("Sending {} failed, notifying connection holder.", req)
      context.parent ! KillConnection
    }

  }

  private object RequestActor {
    def props(req: AthenaRequest, connection: ActorRef, respondTo: ActorRef, requestTimeout: Duration): Props = {
      Props[RequestActor](new RequestActor(req, connection, respondTo, requestTimeout))
    }
  }
}
