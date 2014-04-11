package athena.connector

import akka.actor._
import akka.io._
import akka.io.IO
import athena.connector.pipeline.TcpPipelineHandler.Init
import akka.actor.Terminated

import athena._
import athena.connector.pipeline._
import athena.util.{StreamIDManager, Timestamp}
import athena.connector.pipeline.RequestEnvelope
import athena.connector.pipeline.ResponseEnvelope

import java.nio.ByteOrder
import java.net.InetSocketAddress

import scala.concurrent.duration.{FiniteDuration, Duration, DurationInt}
import scala.collection.mutable

import scala.language.postfixOps
import athena.Responses.{Timedout, RequestFailed}
import athena.connector.CassandraRequests.{CassandraRequest, QueryParams, QueryRequest}
import athena.Requests.{KeyspaceAwareRequest, AthenaRequest}
import athena.connector.CassandraResponses.{Ready, KeyspaceResult, CassandraResponse, ClusterEvent}
import spray.util.LoggingContext

/**
 * An Actor that handles communication for a single connection to a Cassandra server. This Actor responds two main commands,
 * DataCommand and Close (and subclasses thereof). Any failures to either send the request or generate a response
 * will cause a [[athena.Responses.RequestFailed]] message to be returned to the original sender. Additionally, any requests that
 * do not receive responses from the server in the defined request timeout interval will generate a [[athena.Responses.Timedout]]
 * response.
 *
 * This actor will respond to two close commands - [[athena.Athena.Abort]] and [[athena.Athena.Close]].
 *
 * A [[athena.Athena.Close]] command will wait for all active requests to either complete or time out. At this point, the TCP connection to the server
 * will be closed in an orderly fashion, and the sender of the original [[athena.Athena.Close]] will receive a
 * [[athena.Athena.Closed]] message. Any outstanding queued requests will fail with a [[athena.Responses.RequestFailed]] response.
 *
 * An [[athena.Athena.Abort]] command will cause any and all outstanding requests to be immediately terminated with a [[athena.Responses.RequestFailed]]
 * response, and the sender of the [[athena.Athena.Abort]] message will be sent a [[athena.Athena.Aborted]] message.
 *
 * Note - this actor is mainly intended to be used by classes that wrap it with a friendlier interface.
 *
 * @author David Pratt (dpratt@vast.com)
 */
private[athena] class ConnectionActor(connectCommander: ActorRef,
                                      remoteAddress: InetSocketAddress,
                                      settings: ConnectionSettings,
                                      initialKeyspace: Option[String] = None)
  extends Actor with ActorLogging {

  import context.dispatcher
  import context.system

  import ConnectionActor._

  type MyInit = Init[HasConnectionInfo, RequestEnvelope, ResponseEnvelope]

  //Any unhandled crash kills this actor - it models a network connection,
  // so a restart would transparently re-open that connection. That decision should be
  // up to the client of this actor.
  //This strategy will cause any failing child to stop, which should fire a Terminated event to us
  // which we'll either handle or die ourselves due to a death pact.
  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  def receive = openTcpConnection()

  private[this] val eventHandlers = collection.mutable.HashSet.empty[ActorRef]
  private[this] var keyspace: Option[String] = None

  //if false, the connection is saturated and new requests will be bounced
  private[this] var writesEnabled: Boolean = true

  //if true, no new commands that generate network requests will be accepted
  private[this] var exclusiveMode: Boolean = false

  private[this] val defaultQueryParams =
    QueryParams(
      settings.querySettings.defaultConsistencyLevel,
      settings.querySettings.defaultSerialConsistencyLevel,
      None
    )

  private def openTcpConnection(): Receive = {

    val to = settings.socketSettings.connectTimeout
    context.setReceiveTimeout(settings.socketSettings.connectTimeout)
    val tcpTimeout = if(to.isFinite()) {
      Some(FiniteDuration(to.length, to.unit))
    } else {
      None
    }
    IO(Tcp) ! Tcp.Connect(remoteAddress, timeout = tcpTimeout)

    {
      case Tcp.Connected(remote, local) ⇒
        log.debug("Connected to {}", remote)
        val tcpConnection = sender()

        //this defines the event and command processing pipeline
        val stages =
          new MessageStage >>
            new FrameStage >>
            new TcpReadWriteAdapter >>
            new BackpressureBuffer(10000, 1000000, Long.MaxValue)

        val init: MyInit = new Init[HasConnectionInfo, RequestEnvelope, ResponseEnvelope](stages) {
          override def makeContext(ctx: ActorContext): HasConnectionInfo = new HasConnectionInfo {
            override def getLogger = log
            override def getContext = context //use our own ActorContext so that Ticks get sent to us
            def byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
            def host: InetSocketAddress = remote
          }
        }

        val pipelineHandler = context.actorOf(TcpPipelineHandler.props(init, tcpConnection, self).withDeploy(Deploy.local), "pipeline")
        //if the pipeline handler dies, so do we
        context.watch(pipelineHandler)

        //the pipeline handler actor will handle all incoming TCP messages
        //the pipeline will either translate them to Event objects passed back to us
        //or forward on TCP events directly
        sender ! Tcp.Register(pipelineHandler)

        postTcpConnect(init, pipelineHandler, remote, local)

      case Tcp.CommandFailed(_: Tcp.Connect) ⇒
        log.warning("Connection to {} failed with error.", remoteAddress)
        connectCommander ! Athena.ConnectionFailed(remoteAddress)
        context.stop(self)

      case ReceiveTimeout ⇒
        log.warning("Connection to {} timed out.", remoteAddress)
        connectCommander ! Athena.ConnectionFailed(remoteAddress)
        context.stop(self)
    }

  }


  //
  // After the TCP connection has been set up, do the Cassandra handshake and start taking requests
  private def postTcpConnect(init: MyInit, pipelineHandler: ActorRef, remoteAddress: InetSocketAddress, localAddress: InetSocketAddress) {

    type Behavior = PartialFunction[Any, State]

    sealed trait State
    case class NewState(newBehavior: Behavior) extends State
    case object Stay extends State

    /**
     * A utility function to map a partial function to a behavior
     */
    def behavior(b: Behavior): Behavior = b

    /**
     * Use the supplied behavior function to define a state.
     */
    def state(behavior: Behavior): State = NewState(behavior)

    /**
     * Stay in the current state.
     */
    def stay(): State = Stay

    /**
     * Immediately terminate.
     */
    def stop(): State = {
      context.stop(self)
      Stay
    }

    /**
     * Specify the initial state that this actor should start in.
     */
    def startWith(state: State) {
      state match {
        case NewState(newBehavior) => context.become(behavior2Receive(newBehavior))
        case _ =>
          log.error("Invalid starting state! Shutting down.")
          startWith(shutdown())
      }
    }

    /**
     * The default behavior for unhandled messages in any state.
     */
    def defaultBehavior: Behavior = {
      case req: AthenaRequest =>
        log.warning(s"Discarding request $req due to default behavior.")
        sender ! Responses.RequestFailed(req)
        stay()

      case Terminated(`pipelineHandler`) ⇒
        log.warning("Pipeline handler died while waiting for init - stopping")
        connectCommander ! Athena.ConnectionFailed(remoteAddress)
        shutdown(Set(), Athena.Closed, Tcp.Close)

      case cmd: Athena.CloseCommand =>
        log.warning("Closing connection with default behavior.")
        shutdown(Set(sender()), cmd.event, tcpCommandForAthenaCommand(cmd))

    }

    def behavior2Receive(behavior: Behavior): Receive = {
      case x if behavior.isDefinedAt(x) =>
        behavior(x) match {
          case Stay => //do nothing
          case NewState(newBehavior) => context.become(behavior2Receive(newBehavior orElse defaultBehavior))
        }
    }


    def shutdown(commanders: Set[ActorRef] = Set(), response: Athena.ConnectionClosed = Athena.Closed, tcpCloseCommand: Tcp.CloseCommand = Tcp.Close): State = {

      pipelineHandler ! TcpPipelineHandler.Management(tcpCloseCommand)
      context.setReceiveTimeout(settings.socketSettings.connectTimeout)

      def step(cmdrs: Set[ActorRef]): State = state {
        case req: AthenaRequest =>
          log.warning(s"Discarding request $req because connection is closing.")
          sender ! Responses.RequestFailed(req)
          stay()

        case x: Tcp.ConnectionClosed =>
          //we're done
          log.debug("TCP connection closed.")
          commanders.foreach(_ ! response)
          context.unwatch(pipelineHandler)
          stop()

        case Terminated(`pipelineHandler`) ⇒
          //we're done
          commanders.foreach(_ ! response)
          stop()

        case x: Athena.CloseCommand =>
          step(cmdrs + sender)

        case ReceiveTimeout =>
          log.warning("Timed out waiting for TCP close. Stopping actor.")
          commanders.foreach(_ ! response)
          stop()
      }

      step(commanders)
    }


    def initConnection(): State = {

      //TODO - make the error more granular - we need a way to indicate that an
      //error is transient (i.e. can't reach the host) vs. permanently fatal (can't authenticate, bad keyspace, etc)

      def sendStartup(): State = {
        context.setReceiveTimeout(settings.socketSettings.readTimeout)
        pipelineHandler ! init.command(RequestEnvelope(0, CassandraRequests.Startup))
        state {
          case init.Event(ResponseEnvelope(_, CassandraResponses.Ready)) =>
            setInitialKeyspace()
          case init.Event(ResponseEnvelope(_, resp: Athena.Error)) =>
            log.error("Error while connecting - {}", resp)
            connectCommander ! Athena.ConnectionFailed(remoteAddress)
            shutdown()
          case init.Event(ResponseEnvelope(_, resp)) =>
            log.error("Unexpected response {} to connection request.", resp)
            connectCommander ! Athena.ConnectionFailed(remoteAddress)
            shutdown()
          case ReceiveTimeout =>
            log.error("Timed out waiting for READY response.")
            connectCommander ! Athena.ConnectionFailed(remoteAddress)
            shutdown()
        }
      }

      def setInitialKeyspace(): State = {
        if(initialKeyspace.isEmpty) {
          waitingForRegister()
        } else {
          context.setReceiveTimeout(settings.socketSettings.readTimeout)
          pipelineHandler ! init.command(RequestEnvelope(0, QueryRequest(s"USE ${initialKeyspace.get}", defaultQueryParams)))
          state {
            case init.Event(ResponseEnvelope(_, CassandraResponses.KeyspaceResult(ksName))) =>
              keyspace = Some(ksName)
              log.debug("Successfully set keyspace to {}", ksName)
              waitingForRegister()
            case init.Event(ResponseEnvelope(_, resp: CassandraError)) =>
              log.error("Error while setting keyspace - {}", resp)
              connectCommander ! Athena.ConnectionFailed(remoteAddress, Some(resp))
              shutdown()
            case init.Event(ResponseEnvelope(_, resp)) =>
              log.error("Unexpected response {} to keyspace request.", resp)
              connectCommander ! Athena.ConnectionFailed(remoteAddress, Some(Athena.GeneralError("Invalid keyspace response.")))
              shutdown()
            case ReceiveTimeout =>
              log.error("Timed out waiting for keyspace response.")
              connectCommander ! Athena.ConnectionFailed(remoteAddress)
              shutdown()
          }
        }
      }

      def waitingForRegister(): State = {

        context.setReceiveTimeout(settings.socketSettings.readTimeout)
        connectCommander ! Athena.Connected(remoteAddress, localAddress)

        state {
          case Athena.Register(commander) =>
            connectionOpen(commander)

          case ReceiveTimeout =>
            log.error("Connection actor did not receive Register message in time. Shutting down.")
            shutdown()
        }

      }

      sendStartup()
    }

    //ready to take requests
    def connectionOpen(commander: ActorRef): State = {

      context.setReceiveTimeout(Duration.Undefined)

      //death pact with our commander
      context.watch(commander)

      val requestTracker = new RequestMultiplexer(settings.requestTimeout)

      case object Tick
      def scheduleTick() = context.system.scheduler.scheduleOnce(500 millis, self, Tick)

      //schedule our initial tick
      scheduleTick()

      def sendResponse(env: ResponseEnvelope) {
        if(env.streamId < 0) {
          //this is an event - stream IDs < 0 are reserved by the server for this
          env.response match {
            case clusterEvent: ClusterEvent =>
              if(eventHandlers.isEmpty) {
                log.error("Cluster event received from server, but no event handler is defined. Discarding event - {}", clusterEvent)
              } else {
                eventHandlers.foreach(_ ! clusterEvent)
              }
            case x =>
              log.error("Received non-event response with negative stream ID - discarding {}", x)
          }
        } else {
          val ctxOption = requestTracker.removeRequest(env.streamId)
          ctxOption.fold(log.error(s"Discarded response ${env.response} due to unknown stream ID ${env.streamId} - possibly due to timeout.")) { requestContext =>
            val response = (requestContext.command, env.response) match {
              case (RequestCommand(request), resp) =>
                CommandConverters.convertResponse(request, resp)

              case (SetKeyspace(newKeyspace), KeyspaceResult(ksName)) =>
                //FIXME: I hate mutating state here, but it's really the only place I can think of to cleanly do it
                keyspace = Some(ksName)
                KeyspaceChanged(ksName)

              case (SubscribeToEvents, Ready) =>
                eventHandlers.add(requestContext.respondTo)
                ClusterEventsSubscribed

              case (command, x: CassandraError) =>
                ConnectionCommandFailed(command, Some(x))

              case (command, x) =>
                log.error("Unknown response - {} - to request.", x)
                ConnectionCommandFailed(command, Some(Athena.GeneralError("Unknown response to request.")))
            }
            requestContext.respondTo ! response
          }
        }
      }

      def sendRequest(ctx: CommandContext) {
        if(!writesEnabled) {
          ctx.respondTo ! unavailableResponse(ctx.command)
        } else {
          if(exclusiveMode) {
            //we cannot accept any new requests - bounce the current one
            ctx.respondTo ! unavailableResponse(ctx.command)
          } else {
            requestTracker.addRequest(ctx).fold(ctx.respondTo ! errorResponse(ctx.command)) { streamId =>
              pipelineHandler ! init.command(RequestEnvelope(streamId, cassandraRequest(ctx.command)))
            }
          }
        }
      }

      def handleTimeouts() {
        requestTracker.overdueRequests.foreach {
          case (streamId, requestInfo) =>
            requestTracker.removeRequest(streamId).foreach { rCtx =>
              val errorResponse = rCtx.command match {
                case RequestCommand(request) => Timedout(request)
                case x =>
                  log.error("Connection command timed out - {}", x)
                  ConnectionCommandFailed(x)
              }

              rCtx.respondTo ! errorResponse
            }
        }
      }

      def running(): State = {

        state {
          case req: KeyspaceAwareRequest =>
            val requestKs = req.keyspace
            val compatible = if(requestKs.isEmpty) true else {
              keyspace.exists(_ == requestKs.get)
            }
            val respondTo = sender()
            if(!compatible) {
              log.error("Cannot process request on connection with incompatible keyspace. Request ks - {} connection ks - {}", requestKs, keyspace)
              respondTo ! Responses.InvalidRequest(req)
            } else {
              sendRequest(CommandContext(RequestCommand(req), respondTo))
            }
            stay()

          case req: AthenaRequest =>
            val ctx = CommandContext(RequestCommand(req), sender())
            sendRequest(ctx)
            stay()

          case cmd: ConnectionCommand =>
            sendRequest(CommandContext(cmd, sender()))
            stay()

          case UnsubscribeFromEvents =>
            eventHandlers.remove(sender())
            sender() ! ClusterEventsUnsubscribed
            stay()

          case init.Event(env: ResponseEnvelope) =>
            sendResponse(env)
            stay()

          case Tick =>
            scheduleTick()
            handleTimeouts()
            stay()

          case Terminated(`pipelineHandler`) ⇒
            //we need to immediately shut down - this is fatal
            log.debug("Pipeline handler died.")
            stop()

          case Athena.Abort =>
            log.debug("Aborting connection.")
            abort(Set(sender()))

          case Athena.Close =>
            //kill any queued requests, but attempt to finish processing outstanding requests
            log.debug("Closing connection.")
            cleanlyClose(Set(sender()))

          case BackpressureBuffer.HighWatermarkReached ⇒
            //we need to temporarily stop writing requests to the connection
            log.debug("Connection saturated - stopping writes.")
            writesEnabled = false
            stay()

          case BackpressureBuffer.LowWatermarkReached ⇒
            log.debug("Resuming writes.")
            writesEnabled = true
            stay()
        }
      }

      def cleanlyClose(closeCommanders: Set[ActorRef]): State = {

        def closeDone: Boolean = requestTracker.requests.isEmpty

        scheduleTick()

        if(closeDone) {
          shutdown(closeCommanders)
        } else {
          state {
            case req: AthenaRequest =>
              log.warning(s"Discarding request $req because the connection is closing.")
              sender ! Responses.RequestFailed(req)
              stay()

            case init.Event(env: ResponseEnvelope) =>
              sendResponse(env)
              if(closeDone) {
                shutdown(closeCommanders, Athena.Closed, Tcp.Close)
              } else {
                stay()
              }

            case Tick =>
              handleTimeouts()
              if(closeDone) {
                shutdown(closeCommanders, Athena.Closed, Tcp.Close)
              } else {
                scheduleTick()
                stay()
              }

            case Terminated(`pipelineHandler`) ⇒
              log.debug("Pipeline handler died.")
              failAll()
              closeCommanders.foreach(_ ! Athena.Closed)
              stop()

            case Athena.Close =>
              cleanlyClose(closeCommanders + sender)

            case Athena.Abort =>
              abort(closeCommanders + sender)
          }
        }
      }

      def failAll(queued: List[CommandContext] = Nil) {
        requestTracker.requests.foreach { ctx =>
          ctx.respondTo ! errorResponse(ctx.command)
        }
        queued.foreach { ctx =>
          ctx.respondTo ! errorResponse(ctx.command)
        }
      }

      def abort(commander: Set[ActorRef] = Set(), queued: List[CommandContext] = Nil): State = {
        failAll(queued)
        shutdown(commander, Athena.Aborted, Tcp.Abort)
      }

      running()
    }

    startWith(initConnection())
  }

  import athena.ClusterEventName._

  private def cassandraRequest(cmd: ConnectionCommand): CassandraRequest = cmd match {
    case SetKeyspace(newKeyspace) =>
      QueryRequest(s"USE $newKeyspace", defaultQueryParams)

    case SubscribeToEvents =>
      CassandraRequests.Register(Seq(TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE))

    case RequestCommand(request) =>
      CommandConverters.convertRequest(request, settings.querySettings)
  }


  private def errorResponse(cmd: ConnectionCommand) = cmd match {
    case RequestCommand(request) => Responses.RequestFailed(request)
    case x => ConnectionCommandFailed(x)
  }

  private def unavailableResponse(cmd: ConnectionCommand) = cmd match {
    case RequestCommand(request) => Responses.ConnectionUnavailable(request)
    case x => ConnectionCommandFailed(x)
  }

  private class RequestMultiplexer(requestTimeout: Duration)(implicit log: LoggingContext) {

    //manages request stream IDs for requests - Cassandra uses a signed Byte range from 0 -> 127
    //to do request multiplexing - when a request is enqueued, we pick a unique ID to assign to it
    //when the response comes in, we use this ID to determine who to dispatch the response to
    private[this] val streamIdManager = new StreamIDManager

    //Used to hold a mapping of stream IDs to requests. This is used to
    //de-multiplex responses. Each request gets assigned a unique (temporary) stream ID,
    //which the server returns back in the response.
    //Also, we're wrapping a java map since it's quite a bit faster. I hate to have to do this.
    private[this] val activeRequests: mutable.Map[Byte, CommandContext] =
      scala.collection.JavaConversions.mapAsScalaMap(new java.util.HashMap[Byte, CommandContext])

    def openRequestCount: Int = activeRequests.size

    def addRequest(ctx: CommandContext): Option[Byte] = {
      val streamId = streamIdManager.nextId()
      streamId.foreach { id =>
        activeRequests.put(id, ctx)
        if(ctx.command.isExclusive) {
          exclusiveMode = true
        }
      }
      streamId
    }

    def removeRequest(streamId: Byte): Option[CommandContext] = {
      val ctxOpt = activeRequests.remove(streamId)
      ctxOpt.foreach { ctx =>
        streamIdManager.release(streamId)
        if(ctx.command.isExclusive) {
          exclusiveMode = false
        }
      }
      ctxOpt
    }

    def requests: Iterable[CommandContext] = activeRequests.values

    def overdueRequests: Iterable[(Byte, CommandContext)] = activeRequests.filter(_._2.isOverdue(requestTimeout))

  }


}

private[connector] trait HasConnectionInfo extends HasLogging with HasActorContext {
  def byteOrder: ByteOrder
  def host: InetSocketAddress
}

private[connector] object ConnectionActor {


  def props(connectCommander: ActorRef,
            remoteAddress: InetSocketAddress,
            settings: ConnectionSettings,
            initialKeyspace: Option[String] = None): Props = {
    Props(new ConnectionActor(connectCommander, remoteAddress, settings, initialKeyspace))
  }

  //A trait that marks a command that will cause the connection to create a network request.
  // A Connection actor has a slightly larger public API than the other actors in the hierarchy.
  // For example, subscribing to cluster events and setting the keyspace only really make semantic sense
  // (and are possible) on a single, raw connection. When a request comes in, it will be wrapped into a CommandContext
  // appropriate to the command type
  sealed trait ConnectionCommand {
    //If true, no other commands can be executed on the network socket while this request is in flight.
    // Existing responses will be processed, but any new incoming requests will be queued until this
    // response completes.
    def isExclusive: Boolean
  }
 
  case object SubscribeToEvents extends ConnectionCommand {
    override val isExclusive: Boolean = false
  }
  case object ClusterEventsSubscribed

  case object UnsubscribeFromEvents
  case object ClusterEventsUnsubscribed

  case class SetKeyspace(keyspace: String) extends ConnectionCommand {
    override val isExclusive: Boolean = true
  }
  case class KeyspaceChanged(keyspace: String)

  private[ConnectionActor] case class RequestCommand(request: AthenaRequest) extends ConnectionCommand  {
    override val isExclusive: Boolean = false
  }

  //Used to signal that a ConnectionCommand has failed
  // This will be sent when the command is anything but an AthenaRequest - those messages
  // have their own well defined error messages
  case class ConnectionCommandFailed(cmd: ConnectionCommand, error: Option[Athena.Error] = None)

  //used internally to keep track of open requests and responders
  private[ConnectionActor] case class CommandContext(command: ConnectionCommand, respondTo: ActorRef, startTime: Timestamp = Timestamp.now) {
    def isOverdue(timeout: Duration): Boolean = (startTime + timeout).isPast
  }

  private def tcpCommandForAthenaCommand(evt: Athena.CloseCommand): Tcp.CloseCommand = evt match {
    case Athena.Close => Tcp.Close
    case Athena.Abort => Tcp.Abort
  }


}
