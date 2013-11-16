package com.vast.verona.client

import akka.actor._
import akka.io._
import akka.io.IO
import akka.io.TcpPipelineHandler.{WithinActorContext, Init}
import com.vast.verona.client.pipeline.{ResponseEnvelope, RequestEnvelope, FrameStage, MessageStage}
import scala.concurrent.duration.{ Duration, DurationInt }
import com.vast.verona.util.{Timestamp, StreamIDManager}
import com.vast.verona.{Cassandra, InternalException}

class CassandraClientConnection(connectCommander: ActorRef, connect: Cassandra.Connect, settings: ClientSettings) extends Actor with Stash with ActorLogging {

  import context.system

  type MyInit = Init[WithinActorContext, RequestEnvelope, ResponseEnvelope]

  IO(Tcp) ! Tcp.Connect(connect.remoteAddress)

  // we cannot sensibly recover from crashes
  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  //mutable state, but Actors are synchronous.
  private[this] var requestMap: Map[Byte, RequestInfo] = Map.empty[Byte, RequestInfo]
  private[this] val streamIdManager = new StreamIDManager

  private def enqueueRequest(req: Request, sender: ActorRef): RequestEnvelope = {
    val streamId = streamIdManager.nextId()
    if(requestMap.contains(streamId)) {
      log.error(s"Duplicate stream ID generated.")
      throw new InternalException("Internal connection failure.")
    }
    val ri = RequestInfo(streamId, req, sender, Timestamp.now)
    requestMap = requestMap.updated(ri.streamId, ri)
    RequestEnvelope(ri.streamId, req)
  }

  private def dequeueResponse(streamId: Byte): Option[ActorRef] = {
    val riOpt = requestMap.get(streamId)
    streamIdManager.release(streamId)
    requestMap = requestMap - streamId
    riOpt.map(_.sender)
  }


  context.setReceiveTimeout(settings.socketSettings.connectTimeout)
  def receive = {
    case connected@Tcp.Connected(local, remote) ⇒
      log.debug("Connected to {}", remote)
      val tcpConnection = sender

      val init = TcpPipelineHandler.withLogger(log,
        new MessageStage >>
          new FrameStage >>
          new TcpReadWriteAdapter >>
          new BackpressureBuffer(10000, 1000000, Long.MaxValue))

      val pipelineHandler = context.actorOf(TcpPipelineHandler.props(init, tcpConnection, self).withDeploy(Deploy.local), "pipeline")
      //if the pipeline handler dies, so do we
      context.watch(pipelineHandler)

      //the pipeline handler actor will handle all incoming TCP messages
      //the pipeline will either translate them to Event objects passed back to us
      //or forward on TCP events directly
      sender ! Tcp.Register(pipelineHandler)

      log.debug("Connected - sending STARTUP.")
      pipelineHandler ! init.command(RequestEnvelope(0, Startup))

      context.setReceiveTimeout(settings.socketSettings.readTimeout)
      context.become(waitingForInit(connectCommander, init, pipelineHandler, connected))

    case Tcp.CommandFailed(_: Tcp.Connect) ⇒
      connectCommander ! Cassandra.CommandFailed(connect)
      context.stop(self)

    case ReceiveTimeout ⇒
      log.warning("Configured connecting timeout of {} expired, stopping", settings.socketSettings.connectTimeout)
      connectCommander ! Cassandra.CommandFailed(connect)
      context.stop(self)
  }


  def waitingForInit(connectCommander: ActorRef, init: MyInit, pipelineHandler: ActorRef, connectedEvent: Tcp.Connected): Receive = {
    case init.Event(ResponseEnvelope(_, Ready)) =>
      log.debug(s"READY received from server.")
      //server is ready to go!
      connectCommander ! connectedEvent
      context.become(connected(init, pipelineHandler))

    case init.Event(ResponseEnvelope(_, error: Error)) =>
      log.error(s"ERROR while starting up connection - $error")
      connectCommander ! Cassandra.CommandFailed(connect)
      context.stop(self)

    case ReceiveTimeout ⇒
      log.warning("Configured connecting timeout of {} expired, stopping", settings.socketSettings.connectTimeout)
      connectCommander ! Cassandra.CommandFailed(connect)
      context.stop(self)
  }

  def connected(init: MyInit, pipelineHandler: ActorRef): Receive = {
    case req: Request =>
      pipelineHandler ! init.Command(enqueueRequest(req, sender))

    case init.Event(ResponseEnvelope(streamId, response)) ⇒
      val responder = dequeueResponse(streamId)
      responder.fold {
        log.error(s"Discarded response $response due to unknown stream ID $streamId")
      } { destination =>
        destination ! response
      }

    case BackpressureBuffer.HighWatermarkReached ⇒
      context.setReceiveTimeout(5.seconds)
      context.become({
        case BackpressureBuffer.LowWatermarkReached ⇒
          unstashAll()
          context.setReceiveTimeout(Duration.Undefined)
          context.unbecome()
        case ReceiveTimeout ⇒
          log.error("receive timeout while throttled")
          context.stop(self)
        case _ ⇒ stash()
      }, discardOld = false)

    case e: Tcp.ConnectionClosed ⇒
      log.debug("Connection closed!")
      //TODO - add error handling
      context.stop(self)
  }

  override def unhandled(msg: Any): Unit = {
    log.debug(s"Unhandled message $msg")
    super.unhandled(msg)
  }


  private case class RequestInfo(streamId: Byte, request: Request, sender: ActorRef, startTime: Timestamp) {
    def isOverdue(timeout: Duration): Boolean = (startTime + timeout).isPast
  }


}