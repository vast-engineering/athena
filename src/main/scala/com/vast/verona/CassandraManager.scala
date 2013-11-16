package com.vast.verona

import akka.actor._
import com.vast.verona.client.{ClientSettingsGroup, ClientSettings}
import com.vast.verona.Cassandra.NodeConnectorSetup
import com.vast.verona.Cassandra.ClusterConnectorSetup
import akka.actor.Terminated

private[verona] class CassandraManager(cassandraSettings: CassandraExt#Settings) extends Actor with ActorLogging {

  private[this] val groupCounter = Iterator from 0
  private[this] var settingsGroups = Map.empty[ClientSettings, ActorRef]

  private[this] var nodeConnectors = Map.empty[NodeConnectorSetup, ActorRef]
  private[this] var clusterConnectors = Map.empty[ClusterConnectorSetup, ActorRef]

  def receive = withTerminationManagement {
    case connect: Cassandra.Connect =>
      settingsGroupFor(ClientSettings(connect.settings)).forward(connect)

  }

  def settingsGroupFor(settings: ClientSettings): ActorRef = {
    def createAndRegisterSettingsGroup = {
      val group = context.actorOf(
        props = Props[ClientSettingsGroup](newClientSettingsGroup(settings, cassandraSettings)) withDispatcher cassandraSettings.SettingsGroupDispatcher,
        name = "group-" + groupCounter.next())
      settingsGroups = settingsGroups.updated(settings, group)
      context.watch(group)
    }
    settingsGroups.getOrElse(settings, createAndRegisterSettingsGroup)
  }

  def newClientSettingsGroup(settings: ClientSettings, httpSettings: CassandraExt#Settings) =
    new ClientSettingsGroup(settings, httpSettings)


  def withTerminationManagement(behavior: Receive): Receive = ({
    case ev@Terminated(child) ⇒
      if (clusterConnectors.exists(_._2 == child)) {
        clusterConnectors = clusterConnectors filter {
          _._2 != child
        }
      } else if (nodeConnectors exists (_._2 == child)) {
        nodeConnectors = nodeConnectors filter {
          _._2 != child
        }
      } else {
        settingsGroups = settingsGroups filter {
          _._2 != child
        }
      }
      behavior.applyOrElse(ev, (_: Terminated) ⇒ ())
  }: Receive) orElse behavior


}
