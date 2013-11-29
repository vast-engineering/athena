package athena.connector

import akka.actor._
import athena.{Athena, AthenaRequest}

/**
 * Manages a pool of connections to a single Cassandra node. This Actor takes incoming requests and dispatches
 * them to child managed connections, while at the same time monitoring the state of those connections.
 *
 * @author David Pratt (dpratt@vast.com)
 */
private[athena] class NodeConnector(setup: Athena.NodeConnectorSetup) extends Actor with ActorLogging {

  import NodeConnector._

  private[this] var slotStates = Map.empty[ActorRef, SlotState] // state per child
  private[this] var idleConnections = List.empty[ActorRef] // FILO queue of idle connections, managed by updateSlotState
  private[this] var unconnectedConnections = List.empty[ActorRef] // FILO queue of unconnected, managed by updateSlotState
  private[this] val counter = Iterator from 0

  // we cannot sensibly recover from crashes
  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private[this] val settings = setup.settings.get

  override def preStart() {
    for(i <- 0 until settings.poolSettings.coreConnections) {
      newConnectionChild()
    }
  }

  def receive: Receive = {
    case req: AthenaRequest =>
      val connectionOpt = pickConnection()
      if(connectionOpt.isEmpty) {
        sender ! NodeUnavailable(req)
      } else {
        dispatch(req, connectionOpt.get)
      }

    case RequestCompleted ⇒
      log.debug("Request completed.")
      updateSlotState(sender, slotStates(sender).decrementRequestCount)

    case Disconnected(rescheduledRequestCount) ⇒
      val newState =
        slotStates(sender) match {
          case SlotState.Connected(requestCount) if requestCount == rescheduledRequestCount ⇒
            SlotState.Unconnected // "normal" case when a connection was closed

          case SlotState.Idle ⇒ SlotState.Unconnected

          case SlotState.Connected(requestCount) if requestCount > rescheduledRequestCount ⇒
            //this means that the node disconnected but we've sent it requests in the interim
            SlotState.Connected(requestCount - rescheduledRequestCount)

          case SlotState.Unconnected ⇒ throw new IllegalStateException("Unexpected slot state: Unconnected")
        }
      updateSlotState(sender, newState)

    case Terminated(child) ⇒
      removeSlot(child)

    case cmd: Athena.CloseCommand =>
      val stillConnected = slotStates.foldLeft(Set.empty[ActorRef]) {
        case (acc, (_, SlotState.Unconnected)) =>
          acc
        case (acc, (connection, _)) =>
          connection ! Athena.Close
          acc + connection
      }
      if (stillConnected.isEmpty) {
        sender ! Athena.Closed
        context.stop(self)
      } else {
        context.setReceiveTimeout(settings.closeTimeout)
        context.become(closing(cmd, stillConnected, Set(sender)))
      }


  }

  def closing(command: Athena.CloseCommand, connected: Set[ActorRef], commanders: Set[ActorRef]): Receive = {
    case cmd: Athena.CloseCommand =>
      context.become(closing(cmd, connected, commanders + sender))

    case Terminated(child) ⇒
      val stillConnected = connected - child
      if (stillConnected.isEmpty) {
        commanders foreach (_ ! command.event)
        context.stop(self)
      } else context.become(closing(command, stillConnected, commanders))

    case ReceiveTimeout ⇒
      log.warning("Initiating forced shutdown due to close timeout expiring.")
      context.stop(self)

    case _: Disconnected | RequestCompleted  ⇒ // ignore
  }


  def firstIdleConnection: Option[ActorRef] = idleConnections.headOption
  def firstUnconnectedConnection: Option[ActorRef] = unconnectedConnections.headOption orElse {
    if (slotStates.size < settings.poolSettings.maxConnections) Some(newConnectionChild()) else None
  }

  def newConnectionChild(): ActorRef = {
    val child = context.watch {
      context.actorOf(
        props = Props[NodeConnectionHolder](new NodeConnectionHolder(setup.remoteAddress, setup.keyspace, settings.connectionSettings)),
        name = counter.next().toString)
    }
    updateSlotState(child, SlotState.Idle)
    child
  }

  def pickConnection(): Option[ActorRef] = firstIdleConnection orElse firstUnconnectedConnection orElse {
    def available: ((ActorRef, SlotState)) ⇒ Boolean = {
      case (child, x: SlotState.Connected) ⇒ x.openRequestCount < settings.poolSettings.maxConcurrentRequests
      case (child, SlotState.Unconnected) ⇒ true
      case (child, SlotState.Idle) ⇒ true
    }

    slotStates.toSeq.filter(available).sortBy(_._2.openRequestCount).headOption.map(_._1)
  }

  def dispatch(req: AthenaRequest, connection: ActorRef): Unit = {
    connection.forward(req)
    val currentState = slotStates(connection)
    updateSlotState(connection, currentState.incrementRequestCount)
  }

  /** update slot state and manage idleConnections and unconnectedConnections queues */
  def updateSlotState(child: ActorRef, newState: SlotState): Unit = {
    log.debug("Slot states before update- {}", slotStates)

    val oldState = slotStates.get(child)
    slotStates = slotStates.updated(child, newState)

    //a state transition has side effects for us
    (oldState, newState) match {

      case (None, SlotState.Idle) =>
        //this indicates a new connection
        idleConnections ::= child

      case (None, _) ⇒ throw new IllegalStateException // may only change to Idle

      case (Some(SlotState.Unconnected), SlotState.Unconnected) => throw new IllegalStateException

      case (Some(s), SlotState.Unconnected) ⇒
        unconnectedConnections ::= child
        if (s == SlotState.Idle)
          idleConnections = idleConnections.filterNot(_ == child)

      case (Some(SlotState.Connected(_)), SlotState.Idle) ⇒ idleConnections ::= child

      case (Some(SlotState.Unconnected), SlotState.Connected(_)) ⇒
        require(unconnectedConnections.head == child)
        unconnectedConnections = unconnectedConnections.tail

      case (Some(SlotState.Idle), SlotState.Connected(_)) ⇒
        require(idleConnections.head == child)
        idleConnections = idleConnections.tail

      case (Some(SlotState.Connected(_)), SlotState.Connected(_)) ⇒ // ignore
      case (Some(SlotState.Idle), SlotState.Idle) ⇒ throw new IllegalStateException
      case (Some(SlotState.Unconnected), SlotState.Idle) ⇒ throw new IllegalStateException // not possible
    }

    log.debug("Slot states after update- {}", slotStates)
  }

  def removeSlot(child: ActorRef): Unit = {
    slotStates -= child
    unconnectedConnections = unconnectedConnections.filterNot(_ == child)
    idleConnections = idleConnections.filterNot(_ == child)
  }


}

private[connector] object NodeConnector {

  //sent by this Actor to the original sender when all connections to this host are saturated
  case class NodeUnavailable(request: AthenaRequest)

  case class NodeConnected()
  case class Disconnected(rescheduledRequestCount: Int)

  case class RequestCompleted(request: AthenaRequest)

  sealed trait SlotState {
    def incrementRequestCount: SlotState
    def decrementRequestCount: SlotState
    def openRequestCount: Int
  }
  object SlotState {
    sealed private[SlotState] abstract class WithoutRequests extends SlotState {
      def incrementRequestCount = Connected(1)
      def decrementRequestCount = throw new IllegalStateException
      def openRequestCount = 0
    }
    case object Unconnected extends WithoutRequests
    case object Idle extends WithoutRequests

    case class Connected(openRequestCount: Int) extends SlotState {
      require(openRequestCount > 0)
      def incrementRequestCount= Connected(openRequestCount + 1)
      def decrementRequestCount = {
        val newCount = openRequestCount - 1
        if (newCount == 0) Idle
        else Connected(newCount)
      }
    }
  }
}
