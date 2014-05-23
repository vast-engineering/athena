package athena.connector

import akka.testkit.TestActorRef
import org.scalatest.{WordSpecLike, Matchers}
import athena.{Athena, AthenaTest, ClusterConnectorSettings}
import athena.connector.ClusterInfo.ClusterMetadata

import scala.language.postfixOps

import athena.connector.ClusterMonitorActor.ClusterReconnected

class ClusterMonitorSpec extends AthenaTest with WordSpecLike with Matchers {

  //
  // TODO: Add ccm stuff so that we can take down nodes and such.
  //
  val settings = ClusterConnectorSettings(system)

  "The cluster monitor actor" should {
    "connect to the cluster" in {
      val ref = TestActorRef(new ClusterMonitorActor(self, hosts, port, settings))
      expectMsg(ClusterReconnected)
      expectMsgType[ClusterMetadata]
      akka.pattern.gracefulStop(ref, timeout.duration, Athena.Close)
    }
  }

}