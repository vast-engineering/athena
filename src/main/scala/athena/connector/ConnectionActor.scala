package athena.connector

import akka.actor._
import akka.io._
import akka.io.IO
import akka.io.TcpPipelineHandler.Init
import akka.actor.Terminated

import athena._
import athena.connector.pipeline._
import athena.util.{StreamIDManager, Timestamp}
import athena.connector.pipeline.RequestEnvelope
import athena.connector.pipeline.ResponseEnvelope

import java.nio.ByteOrder
import java.net.InetSocketAddress

import scala.concurrent.duration.{Duration, DurationInt}
import scala.collection.mutable

import scala.language.postfixOps
import athena.Responses.{Timedout, RequestFailed}
import athena.connector.CassandraRequests.QueryRequest
import athena.Requests.AthenaRequest
import athena.connector.CassandraResponses.ClusterEvent

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
class ConnectionActor(connectCommander: ActorRef, connect: Athena.Connect,
                      settings: ConnectionSettings)
  extends Actor with ActorLogging {

  import context.dispatcher
  import context.system

  import ConnectionActor._

  type MyInit = Init[HasConnectionInfo, RequestEnvelope, ResponseEnvelope]

  // we cannot sensibly recover from crashes
  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  def receive = openTcpConnection()

  private def openTcpConnection(): Receive = {

    context.setReceiveTimeout(settings.socketSettings.connectTimeout)
    IO(Tcp) ! Tcp.Connect(connect.remoteAddress)

    {
      case connected@Tcp.Connected(remote, local) ⇒
        log.debug("Connected to {}", remote)
        val tcpConnection = sender

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
        connectCommander ! Athena.CommandFailed(connect)
        context.stop(self)

      case ReceiveTimeout ⇒
        log.warning("Configured connecting timeout of {} expired, stopping", settings.socketSettings.connectTimeout)
        connectCommander ! Athena.CommandFailed(connect)
        context.stop(self)
    }

  }


  //
  // After the TCP connection has been set up, do the Cassandra handshake and start taking requests
  private def postTcpConnect(init: MyInit, pipelineHandler: ActorRef, remoteAddress: InetSocketAddress, localAddress: InetSocketAddress) {

    def closeTcpConnection(commanders: Set[ActorRef] = Set(), response: Athena.ConnectionClosed, tcpCloseCommand: Tcp.CloseCommand) {

      pipelineHandler ! tcpCloseCommand
      context.become(step(commanders))
      context.setReceiveTimeout(settings.socketSettings.connectTimeout)

      def step(cmdrs: Set[ActorRef]): Receive = {
        case x: Tcp.ConnectionClosed =>
          //we're done
          commanders.foreach(_ ! response)
          context.stop(self)

        case Terminated(`pipelineHandler`) ⇒
          //we're done
          commanders.foreach(_ ! response)
          context.stop(self)

        case x: Athena.CloseCommand =>
          step(cmdrs + sender)

        case ReceiveTimeout =>
          log.warning("Timed out waiting for TCP close. Stopping actor.")
          commanders.foreach(_ ! response)
          context.stop(self)
      }
    }

    val defaultHandler: Receive = {
      case init.Event(ResponseEnvelope(_, err: CassandraError)) =>
        log.error("Pipeline not initialized due to error {}", err)
        connectCommander ! Athena.CommandFailed(connect)
        closeTcpConnection(Set(), Athena.Closed, Tcp.Close)

      case Terminated(`pipelineHandler`) ⇒
        log.error("Pipeline handler died while waiting for init - stopping")
        connectCommander ! Athena.CommandFailed(connect)
        closeTcpConnection(Set(), Athena.Closed, Tcp.Close)

      case ReceiveTimeout ⇒
        log.warning("Configured connecting timeout of {} expired, stopping", settings.socketSettings.connectTimeout)
        connectCommander ! Athena.CommandFailed(connect)
        closeTcpConnection(Set(), Athena.Closed, Tcp.Close)
    }

    def updateBehavior(behavor: Receive) {
      context.become(behavor orElse defaultHandler)
    }



    def initConnection() {
      pipelineHandler ! init.command(RequestEnvelope(0, CassandraRequests.Startup))
      context.setReceiveTimeout(settings.socketSettings.readTimeout)

      def setupEvents() {
        if(connect.eventHandler.isDefined) {
          import ClusterEventName._
          pipelineHandler ! init.command(RequestEnvelope(0, CassandraRequests.Register(Seq(TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE))))
          context.setReceiveTimeout(settings.socketSettings.readTimeout)
          updateBehavior {
            case init.Event(ResponseEnvelope(_, CassandraResponses.Ready)) =>
              setupKeyspace()
          }
        } else {
          setupKeyspace()
        }
      }

      def setupKeyspace() {

        if(connect.keyspace.isEmpty) {
          connectionPrepared()
        } else {

          val setKsQuery = QueryRequest(s"USE ${connect.keyspace.get}",
            settings.querySettings.defaultConsistencyLevel,
            settings.querySettings.defaultSerialConsistencyLevel,
            None)

          pipelineHandler ! init.command(RequestEnvelope(0, setKsQuery))
          updateBehavior {
            case init.Event(ResponseEnvelope(_, CassandraResponses.KeyspaceResult(ksName))) =>
              log.debug("Successfully set keyspace to {}", ksName)
              connectionPrepared()
          }
        }
      }

      def connectionPrepared() {
        //server is ready to go!
        context.setReceiveTimeout(Duration.Undefined)
        connectCommander ! Athena.Connected(remoteAddress, localAddress)
        context.become(connectionOpen())
      }

      updateBehavior {
        case init.Event(ResponseEnvelope(_, CassandraResponses.Ready)) =>
          setupEvents()
      }
    }

    //ready to take requests
    def connectionOpen(): Receive = {

      val requestTracker = new RequestMultiplexer(settings.requestTimeout)

      case object Tick
      def scheduleTick() = context.system.scheduler.scheduleOnce(500 millis, self, Tick)

      def sendRequest(ctx: ConnectionRequestContext) {
        requestTracker.addRequest(ctx).fold(sender ! RequestFailed(ctx.request)) { streamId =>
          pipelineHandler ! init.command(RequestEnvelope(streamId, CommandConverters.convertRequest(ctx.request, settings.querySettings)))
        }
      }

      def sendResponse(env: ResponseEnvelope) {
        if(env.streamId < 0) {
          //this is an event - stream IDs < 0 are reserved by the server for this
          env.response match {
            case clusterEvent: ClusterEvent =>
              connect.eventHandler.fold(log.error("Cluster event received from server, but no event handler is defined. Discarding event - {}", clusterEvent)) { handler =>
                handler ! clusterEvent
              }
            case x =>
              log.error("Received non-event response with negative stream ID - discarding {}", x)
          }
        } else {
          val ctxOption = requestTracker.removeRequest(env.streamId)
          ctxOption.fold(log.error(s"Discarded response ${env.response} due to unknown stream ID ${env.streamId} - possibly due to timeout.")) { requestContext =>
            requestContext.respondTo ! CommandConverters.convertResponse(requestContext.request, env.response)
          }
        }
      }

      def connected(writesEnabled: Boolean = true, queued: List[ConnectionRequestContext] = Nil): Receive = {
        case req: AthenaRequest =>
          val ctx = ConnectionRequestContext(req, sender)
          if(writesEnabled) {
            sendRequest(ctx)
          } else {
            context.become(connected(writesEnabled, ctx :: queued))
          }

        case init.Event(env: ResponseEnvelope) =>
          sendResponse(env)

        case Tick =>
          requestTracker.checkForTimeouts()
          scheduleTick()

        case BackpressureBuffer.HighWatermarkReached ⇒
          //we need to temporarily stop writing requests to the connection
          log.debug("Connection saturated - stopping writes.")
          context.become(connected(writesEnabled = false, queued))

        case BackpressureBuffer.LowWatermarkReached ⇒
          log.debug("Resuming writes.")
          queued foreach sendRequest
          context.become(connected(writesEnabled = true, Nil))

        case Terminated(`pipelineHandler`) ⇒
          //we need to immediately shut down - this is fatal
          log.debug("Pipeline handler died.")
          failAll(queued)
          context.stop(self)

        case Athena.Abort =>
          abort(Set(sender), queued)

        case Athena.Close =>
          //kill any queued requests, but attempt to finish processing outstanding requests
          queued.foreach { ctx =>
            ctx.respondTo ! RequestFailed(ctx.request)
          }
          context.become(closing(Set(sender)))
      }

      def closing(closeCommanders: Set[ActorRef]): Receive = {
        case req: AthenaRequest =>
          log.warning(s"Discarding request $req because the connection is closing.")

        case init.Event(env: ResponseEnvelope) =>
          sendResponse(env)
          if(requestTracker.requests.isEmpty) {
            closeTcpConnection(closeCommanders, Athena.Closed, Tcp.Close)
          }

        case Tick =>
          requestTracker.checkForTimeouts()
          if(requestTracker.requests.isEmpty) {
            closeTcpConnection(closeCommanders, Athena.Closed, Tcp.Close)
          } else {
            scheduleTick()
          }

        case Terminated(`pipelineHandler`) ⇒
          log.debug("Pipeline handler died.")
          failAll()
          closeCommanders.foreach(_ ! Athena.Closed)

        case Athena.Close =>
          closing(closeCommanders + sender)

        case Athena.Abort =>
          abort(closeCommanders + sender)

      }

      def failAll(queued: List[ConnectionRequestContext] = Nil) {
        requestTracker.requests.foreach { ctx =>
          ctx.respondTo ! RequestFailed(ctx.request)
        }
        queued.foreach { ctx =>
          ctx.respondTo ! RequestFailed(ctx.request)
        }
      }

      def abort(commander: Set[ActorRef] = Set(), queued: List[ConnectionRequestContext] = Nil) = {
        failAll(queued)
        closeTcpConnection(commander, Athena.Aborted, Tcp.Abort)
      }

      //schedule our initial tick
      scheduleTick()
      connected()
    }

    initConnection()

  }

}

private[connector] trait HasConnectionInfo extends HasLogging with HasActorContext {
  def byteOrder: ByteOrder
  def host: InetSocketAddress
}

private[connector] object ConnectionActor {

  //used internally to keep track of open requests and responders
  private[ConnectionActor] case class ConnectionRequestContext(request: AthenaRequest, respondTo: ActorRef, startTime: Timestamp = Timestamp.now) {
    def isOverdue(timeout: Duration): Boolean = (startTime + timeout).isPast
  }

  private[ConnectionActor] class RequestMultiplexer(requestTimeout: Duration) {

    //manages request stream IDs for requests - Cassandra uses a signed Byte range from 0 -> 127
    //to do request multiplexing - when a request is enqueued, we pick a unique ID to assign to it
    //when the response comes in, we use this ID to determine who to dispatch the response to
    private[this] val streamIdManager = new StreamIDManager

    //Used to hold a mapping of stream IDs to requests. This is used to
    //de-multiplex responses. Each request gets assigned a unique (temporary) stream ID,
    //which the server returns back in the response.
    //Also, we're wrapping a java map since it's quite a bit faster. I hate to have to do this.
    private[this] val activeRequests: mutable.Map[Byte, ConnectionRequestContext] =
      scala.collection.JavaConversions.mapAsScalaMap(new java.util.HashMap[Byte, ConnectionRequestContext])

    def addRequest(ctx: ConnectionRequestContext): Option[Byte] = {
      val streamId = streamIdManager.nextId()
      streamId.foreach(activeRequests.put(_, ctx))
      streamId
    }

    def removeRequest(streamId: Byte): Option[ConnectionRequestContext] = {
      val ctxOpt = activeRequests.remove(streamId)
      ctxOpt.foreach { ctx =>
        streamIdManager.release(streamId)
      }
      ctxOpt
    }

    def requests: Iterable[ConnectionRequestContext] = activeRequests.values

    def checkForTimeouts() {
      activeRequests.filter(_._2.isOverdue(requestTimeout)).foreach {
        case (streamId, requestInfo) =>
          removeRequest(streamId).foreach { rCtx =>
            rCtx.respondTo ! Timedout(rCtx.request)
          }
      }
    }

  }

}
