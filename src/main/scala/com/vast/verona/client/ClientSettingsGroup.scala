package com.vast.verona.client

import com.vast.verona.{Cassandra, CassandraExt}
import akka.actor.{ActorRef, Props, Actor}

private[verona] class ClientSettingsGroup(settings: ClientSettings, cassandraSettings: CassandraExt#Settings) extends Actor {

  val connectionCounter = Iterator from 0

  def receive = {
    case connect: Cassandra.Connect =>
      val commander = sender
      context.actorOf(
        props = Props[CassandraClientConnection](new CassandraClientConnection(commander, connect, settings))
          .withDispatcher(cassandraSettings.ConnectionDispatcher),
        name = connectionCounter.next().toString)
    case Cassandra.CloseAll(cmd) ⇒
      val children = context.children.toSet
      if (children.isEmpty) {
        sender ! Cassandra.ClosedAll
        context.stop(self)
      } else {
        children foreach { _ ! cmd }
        context.become(closing(children, Set(sender)))
      }

  }

  def closing(children: Set[ActorRef], commanders: Set[ActorRef]): Receive = {
    case _: Cassandra.CloseAll ⇒
      context.become(closing(children, commanders + sender))

    case _: Cassandra.ConnectionClosed ⇒
      val stillRunning = children - sender
      if (stillRunning.isEmpty) {
        commanders foreach (_ ! Cassandra.ClosedAll)
        context.stop(self)
      } else context.become(closing(stillRunning, commanders))
  }

}
