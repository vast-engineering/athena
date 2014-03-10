package athena.connector

import akka.actor._
import akka.actor.Terminated

import athena.Athena._
import athena.{NodeConnectorSettings, Responses, Athena}
import athena.Requests.AthenaRequest
import athena.Responses.AthenaResponse
import athena.Responses.ConnectionUnavailable
import athena.Athena.NodeDisconnected
import athena.Athena.NodeConnected

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Manages a pool of connections to a single Cassandra node. This Actor takes incoming requests and dispatches
 * them to child managed connections, while at the same time monitoring the state of those connections.
 *
 * @author David Pratt (dpratt@vast.com)
 */
private[athena] class NodeConnector(commander: ActorRef,
                                    remoteAddress: InetSocketAddress,
                                    keyspace: Option[String],
                                    settings: NodeConnectorSettings) extends Actor with Stash with ActorLogging {

  //
  // TODO - reap connections below the usage threshold
  //

  import NodeConnector._

  import context.dispatcher

  private[this] val counter = Iterator from 0

  private val defaultBehavior: Receive = {
    case req: AthenaRequest =>
      sender ! ConnectionUnavailable(req)
      
    case close: Athena.CloseCommand =>
      log.warning("Unhandled close request to node connector. Shutting down.")
      sender ! close.event
      context.stop(self)    
  }


  override def preStart() = {
    //start in the connecting state
    connecting(spawnNewConnection())
  }

  def receive: Receive = defaultBehavior

  //if retryCount is non-negative, we are attempting to reconnect after a disconnection.
  //this method attempts to connect to just a single host - if it's successful, we then open
  //the rest of the connections
  private def connecting(connection: ActorRef, retryCount: Int = -1) {

    context.setReceiveTimeout(settings.poolSettings.connectionTimeout)

    val behavior: Receive = {
      case Athena.Connected(_, _) if sender == connection =>
        if(retryCount > -1) {
          log.info("Reconnected to host {}", remoteAddress.getAddress)
        } else {
          log.info("Connected to host {}", remoteAddress.getAddress)
        }
        connected(connection)

      case Athena.CommandFailed(Athena.Connect(_, _, _, _), None) =>
        //no attached error to the failed response means we can try to recover
        closeConnection(connection)
        reconnecting(retryCount + 1)

      case x@Athena.CommandFailed(Athena.Connect(_, _, _, _), Some(error)) =>
        closeConnection(connection)
        commander ! NodeFailed(remoteAddress.getAddress, error)
        //now kill ourselves - we cannot connect
        context.stop(self)

      case Terminated(`connection`)  ⇒
        reconnecting(retryCount + 1)

      case cmd: Athena.CloseCommand =>
        close(Set(connection), cmd, Set(sender))

      case ReceiveTimeout =>
        log.debug("Timed out waiting for core connection to host {} in connecting state.", remoteAddress.getAddress)
        closeConnection(connection)
        reconnecting(retryCount + 1)
    } 
    
    context.become(behavior orElse defaultBehavior)
  }

  private def disconnected() = {

    context.setReceiveTimeout(Duration.Inf)

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
        connecting(spawnNewConnection(), 0)

      case InternalReconnect =>
        connecting(spawnNewConnection(), retryCount)
    }

    context.become(behavior orElse defaultBehavior)
  }

  private def connected(baseConnection: ActorRef) {

    context.setReceiveTimeout(Duration.Inf)

    //notify the cluster that we've connected
    commander ! NodeConnected(remoteAddress.getAddress)

    //spawn the rest of the connections in the pool
    val newConnections: Seq[ActorRef] = (1 until settings.poolSettings.coreConnections).map { index =>
      spawnNewConnection()
    }
    
    val allConnectionStatus: Map[ActorRef, ConnectionStatus] = ((baseConnection -> Connected(baseConnection)) +: newConnections.map { conn =>
      conn -> Connecting(conn)
    }).toMap

    def disconnect(conns: Set[ActorRef]) = {
      log.debug("Disconnecting pool from host {}", remoteAddress.getAddress)
      conns.foreach(closeConnection)
      disconnected()
    }

    def step(connections: Map[ActorRef, ConnectionStatus]) {

      def openConnections: Iterable[Connected] = connections.values.collect {
        case x: Connected => x
      }

      def dispatch(req: AthenaRequest): Map[ActorRef, ConnectionStatus] = {
        //find the least busy active connection and it's current request count
        val active = openConnections
        val leastBusy = active.reduceLeftOption[Connected] {
          case (acc, connection) => if(connection.openRequestCount < acc.openRequestCount) connection else acc
        }
        leastBusy.map { connected =>

          //if the least busy connection has more than the max number of simultaneous requests,
          //and we have fewer than the max number of connections, spawn another one
          val possiblyNewState =
            if (connected.openRequestCount > settings.poolSettings.maxConcurrentRequests &&
              active.size < settings.poolSettings.maxConnections) {

              val newConnection = spawnNewConnection()
              connections.updated(newConnection, Connecting(newConnection))

          } else {
            connections
          }

          //fire off a request actor
          context.actorOf(
            props = RequestActor.props(req, connected.connection, sender, settings.connectionSettings.requestTimeout),
            name = "request-actor-" + counter.next()
          )

          //increment the connection count
          //just be paranoid
          if(!possiblyNewState.contains(connected.connection)) {
            throw new IllegalStateException("Cannot find entry for connection.")
          } else {
            possiblyNewState.updated(connected.connection, connected.copy(openRequestCount = connected.openRequestCount + 1))
          }
        } getOrElse {
          //we don't have any available connections - signal that to our sender
          sender ! ConnectionUnavailable(req)
          connections
        }
      }

      def decrementConnection(connection: ActorRef): Map[ActorRef, ConnectionStatus] = {
        val connState = connections.getOrElse(connection, throw new IllegalStateException("Cannot find entry for connection."))
        val newState = connState match {
          case Connected(internalConn, requestCount) =>
            if(requestCount <= 0) {
              throw new IllegalStateException("Cannot decrement connection request count - new count would be negative.")
            } else {
              Connected(internalConn, requestCount - 1)
            }
          case _ =>
            throw new IllegalStateException("Cannot decrement connection request count - connection not marked as active.")
        }
        connections.updated(connection, newState)
      }

      val behavior: Receive = {
        case Athena.Connected(remote, local) =>
          val connection = sender
          log.debug("Host {} adding connection {} to pool", remote.getAddress, connection)
          //sanity check
          if(!connections.contains(connection)) {
            throw new IllegalStateException("Cannot add connected entry for actor - no record of it's connection attempt.")
          }
          step(connections.updated(connection, Connected(connection, 0)))

        case Athena.CommandFailed(Athena.Connect(remoteHost, _, _, _), _) =>
          disconnect(connections.keySet)

        case Terminated(child) if connections.contains(child) =>
          val newConns = connections - child
          disconnect(newConns.keySet)

        case req: AthenaRequest =>
          step(dispatch(req))

        case Disconnect =>
          disconnect(connections.keySet)

        case RequestCompleted(connection) ⇒
          log.debug("Request completed.")
          val newState = decrementConnection(connection)
          step(newState)

        case cmd: Athena.CloseCommand =>
          close(connections.keySet, cmd, Set(sender))

      }
      
      context.become(behavior orElse defaultBehavior)
    }
    
    
    step(allConnectionStatus)
  }

  private def close(liveConnections: Set[ActorRef], command: Athena.CloseCommand, commanders: Set[ActorRef]) {

    context.setReceiveTimeout(Duration.Inf)

    log.debug("Closing node connector with active connections - {}", liveConnections)


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

          case Athena.Connected(_, _) =>
            closing(closeActors + shutdownConnection(sender, command), command, commanders)

          case Athena.CommandFailed(Athena.Connect(remoteHost, _, _, _), _) =>
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
    //watch the close actors
    val closeActors = liveConnections.map{ connection =>
      context.watch {
        shutdownConnection(connection, command)
      }
    }

    closing(closeActors, command, commanders)
  }

  private def spawnNewConnection(): ActorRef = {
    log.debug("Spawning connection.")
    val connect = Athena.Connect(remoteAddress, keyspace, Some(settings.connectionSettings), None)
    context.watch {
      context.actorOf(
        props = Props(new ConnectionActor(self, connect, settings.connectionSettings)),
        name = "connection-" + counter.next().toString)
    }
  }

  private def shutdownConnection(connection: ActorRef, cmd: Athena.CloseCommand): ActorRef = {
    cmd match {
      case Athena.Abort => abortConnection(connection)
      case Athena.Close => closeConnection(connection)
    }
  }

  private def closeConnection(connection: ActorRef): ActorRef = {
    context.unwatch(connection)
    context.actorOf(CloseActor.props(connection, Athena.Close, settings.connectionSettings.socketSettings.connectTimeout))
  }
  
  private def abortConnection(connection: ActorRef): ActorRef = {
    context.unwatch(connection)
    context.actorOf(CloseActor.props(connection, Athena.Abort, settings.connectionSettings.socketSettings.connectTimeout))    
  }
  
}

private[athena] object NodeConnector {


  def props(commander: ActorRef,
            remoteAddress: InetSocketAddress,
            keyspace: Option[String],
            settings: NodeConnectorSettings): Props = {
    Props(new NodeConnector(commander, remoteAddress, keyspace, settings))
  }

  def props(commander: ActorRef, setup: NodeConnectorSetup): Props = props(commander, setup.remoteAddress, setup.keyspace, setup.settings.get)

  //used internally - this recconnect message won't reset the retry count
  private case object InternalReconnect
  
  //internal messaging events  
  private case class RequestCompleted(connection: ActorRef)

  private sealed trait ConnectionStatus
  private case class Connecting(connection: ActorRef) extends ConnectionStatus
  private case class Connected(connection: ActorRef, openRequestCount: Int = 0) extends ConnectionStatus

  //reconnect policy
  private val BaseRecoDelay: Long = 1000 //1 second
  private val MaxRecoDelay: Long = 10 * 60 * 1000 //10 minutes
  private def reconnectDelay(retryCount: Int = 0): FiniteDuration = {
    //this calculates an exponential reconnection delay dropoff
    Duration(math.min(BaseRecoDelay * (1L << retryCount), MaxRecoDelay), TimeUnit.MILLISECONDS)
    //Duration(1, TimeUnit.MINUTES)
  }


  private class RequestActor(req: AthenaRequest, connection: ActorRef, respondTo: ActorRef, requestTimeout: Duration) extends Actor with ActorLogging {

    //request actors should not restart - they are fire and forget, one and done
    override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

    override def preStart(): Unit = {
      context.watch(connection)
      context.setReceiveTimeout(requestTimeout)
      connection ! req
    }

    def receive: Actor.Receive = {
      case resp: AthenaResponse =>
        //log.debug("Delivering {} for {}", resp, req)
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
      context.parent ! Disconnect
    }

  }

  private object RequestActor {
    def props(req: AthenaRequest, connection: ActorRef, respondTo: ActorRef, requestTimeout: Duration): Props = {
      Props[RequestActor](new RequestActor(req, connection, respondTo, requestTimeout))
    }
  }
}
