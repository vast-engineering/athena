package athena.connector

import akka.testkit.{TestActorRef, ImplicitSender, DefaultTimeout, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{WordSpec, BeforeAndAfterAll, Matchers, WordSpecLike}
import athena.{Athena, AthenaTest, ClusterConnectorSettings}
import scala.concurrent.duration._
import athena.connector.ClusterInfo.ClusterMetadata

import scala.language.postfixOps

import athena.TestData._
import athena.connector.ClusterMonitorActor.ClusterReconnected

class ClusterMonitorSpec extends WordSpec with AthenaTest with Matchers {

  //
  // TODO: Add ccm stuff so that we can take down nodes and such.
  //
  val settings = ClusterConnectorSettings(system)

  "The cluster monitor actor" should {
    "connect to the cluster" in {
      val ref = TestActorRef(new ClusterMonitorActor(self, Hosts, Port, settings))
      expectMsg(ClusterReconnected)
      expectMsgType[ClusterMetadata]
      akka.pattern.gracefulStop(ref, timeout.duration, Athena.Close)
    }
  }

}