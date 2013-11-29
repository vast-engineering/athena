package athena.connector

import akka.actor._
import athena._
import athena.connector.NodeConnector.RequestCompleted
import athena.connector.NodeConnector.Disconnected
import scala.Some
import akka.actor.Terminated
import akka.io.IO
import java.net.InetSocketAddress


/**
 * An Actor responsible for brokering requests to an individual server connection, monitoring and reporting the status
 * of it's connection to the parent, and for dispatching responses.
 * *
 * @author David Pratt (dpratt@vast.com)
 */
class NodeConnectionHolder(remoteAddress: InetSocketAddress, keyspace: Option[String], connectionSettings: ConnectionSettings)
  extends Actor with Stash with ActorLogging {

  import context.system

  import NodeConnectionHolder._

  //Crashes kill everything
  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  //start the connection process right when we start up
  override def preStart(): Unit = {
    IO(Athena) ! Athena.Connect(remoteAddress, keyspace, Some(connectionSettings))
  }

  def receive: Receive = connecting(Map())

  def unconnected: Receive = {
    case req: AthenaRequest =>
      log.debug("Attempting new connection to {}:{}", remoteAddress.getHostString, remoteAddress.getPort)
      IO(Athena) ! Athena.Connect(remoteAddress, keyspace, Some(connectionSettings))
      context.become(connecting(Map(req -> RequestContext(req, sender))))

    case _: Athena.CloseCommand ⇒ context.stop(self)
  }

  def connecting(openRequests: Map[AthenaRequest, RequestContext], aborted: Option[Athena.CloseCommand] = None): Receive = {
    case _: Athena.Connected if aborted.isDefined ⇒
      sender ! aborted.get
      openRequests.values foreach clear
      context.become(terminating(context.watch(sender)))

    case _: Athena.Connected ⇒
      log.debug("Connection to {}:{} established, dispatching {} pending requests",
        remoteAddress.getHostString, remoteAddress.getPort, openRequests.size)
      openRequests.values foreach sendRequest(sender)
      context.become(connected(context.watch(sender), openRequests))

    case req: AthenaRequest => context.become(connecting(openRequests.updated(req, RequestContext(req, sender))))

    case cmd: Athena.CloseCommand ⇒ context.become(connecting(openRequests, aborted = Some(cmd)))

    case Athena.CommandFailed(cmd) ⇒
      log.debug("Connection attempt failed")
      openRequests.values foreach clear
      if (aborted.isEmpty) {
        context.parent ! Disconnected(openRequests.size)
        context.become(unconnected)
      } else context.stop(self)
  }

  def connected(connection: ActorRef, openRequests: Map[AthenaRequest, RequestContext]): Receive = {
    case req: AthenaRequest =>
      val ctx = RequestContext(req, sender)
      sendRequest(connection)(ctx)
      context.become(connected(connection, openRequests.updated(req, ctx)))

    case resp: AthenaResponse =>
      val requestCtxOpt = openRequests.get(resp.request)
      requestCtxOpt.foreach { ctx =>
        sendResponse(ctx, resp)
        context.parent ! RequestCompleted

        if(resp.isFailure) {
          log.debug("Sending {} failed, closing connection", ctx.request)
          context.become(closing(connection, openRequests - ctx.request))
        } else {
          context.become(connected(connection, openRequests - ctx.request))
        }
      }
      if(requestCtxOpt.isEmpty) {
        log.warning("Received unexpected response for non-existing request: {}, dropping", resp)
      }

    case cmd: Athena.CloseCommand ⇒
      connection ! cmd
      openRequests.values foreach clear
      context.become(terminating(connection))

    case ev: Athena.ConnectionClosed ⇒
      reportDisconnection(openRequests)
      context.become(waitingForConnectionTermination(connection))

    case Terminated(`connection`) ⇒
      reportDisconnection(openRequests)
      context.become(unconnected)
  }

  def closing(connection: ActorRef, openRequests: Map[AthenaRequest, RequestContext]): Receive = {
    case _: Athena.ConnectionClosed ⇒
      reportDisconnection(openRequests)
      context.become(waitingForConnectionTermination(connection))

    case Terminated(`connection`) ⇒
      reportDisconnection(openRequests)
      unstashAll()
      context.become(unconnected)

    case x ⇒ stash()
  }

  def waitingForConnectionTermination(connection: ActorRef): Receive = {
    case Terminated(`connection`) ⇒
      unstashAll()
      context.become(unconnected)
    case x ⇒ stash()
  }

  def terminating(connection: ActorRef): Receive = {
    case _: Athena.ConnectionClosed     ⇒ // ignore
    case Terminated(`connection`) ⇒ context.stop(self)
  }

  def reportDisconnection(openRequests: Map[AthenaRequest, RequestContext]): Unit = {
    context.parent ! Disconnected(openRequests.size)
    openRequests.values foreach clear
  }


  def clear(ctx: RequestContext) {
    ctx.respondTo ! Responses.RequestFailed(ctx.request)
  }

  def sendRequest(connection: ActorRef)(ctx: RequestContext): Unit = {
    if (log.isDebugEnabled) log.debug("Dispatching {} across connection {}", ctx.request, connection)
    connection ! ctx.request
  }

  def sendResponse(requestContext: RequestContext, message: Any): Unit = {
    val RequestContext(request, respondTo) = requestContext
    log.debug("Delivering {} for {}", message, request)
    respondTo ! message
  }
}

object NodeConnectionHolder {
  private case class RequestContext(request: AthenaRequest, respondTo: ActorRef)
}