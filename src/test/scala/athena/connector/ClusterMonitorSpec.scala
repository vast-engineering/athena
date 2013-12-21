package athena.connector

import akka.testkit.{TestActorRef, ImplicitSender, DefaultTimeout, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import athena.{ClusterConnectorSettings, TestLogging}
import scala.concurrent.duration._
import athena.connector.ClusterInfo.ClusterMetadata

import scala.language.postfixOps

import athena.TestData._

class ClusterMonitorSpec extends TestKit(ActorSystem("test")) with WordSpecLike
with DefaultTimeout with ImplicitSender
with Matchers with BeforeAndAfterAll with TestLogging {

  //
  // TODO: Add ccm stuff so that we can take down nodes and such.
  //

  override def afterAll() {
    shutdown(system)
  }

  val settings = ClusterConnectorSettings(system)

  "The cluster monitor actor" should {
    "connect to the cluster" in {
      TestActorRef(new ClusterMonitorActor(self, Hosts, Port, settings))
      within(60 seconds) {
        expectMsgType[ClusterMetadata]
      }
    }
  }


}