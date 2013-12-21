package athena.connector

import akka.testkit._
import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import athena.{Athena, TestLogging}
import scala.concurrent.duration._
import akka.io.IO
import athena.client.Pipelining
import scala.Some
import java.util.concurrent.TimeUnit
import akka.util.Timeout
import scala.concurrent.Await

import athena.TestData._

class ClusterUtilsSpec extends TestKit(ActorSystem("test")) with WordSpecLike
with TestLogging
with DefaultTimeout with ImplicitSender
with Matchers with BeforeAndAfterAll with ClusterUtils {

  override def afterAll() {
    shutdown(system)
  }

  import system.dispatcher

  private[this] val timeoutDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  private[this] implicit val timeout = Timeout(timeoutDuration)

  val host = Hosts.head

  "The cluster utils" should {
    "get the cluster info" in {
      val pipeline = within(timeoutDuration) {
        IO(Athena) ! Athena.NodeConnectorSetup(host.getHostAddress, Port, Some("testks"), None)
        expectMsgType[Athena.NodeConnectorInfo]
        Pipelining.queryPipeline(lastSender)
      }

      val res = Await.result(updateClusterInfo(host, pipeline), timeoutDuration)
      log.debug("Cluster info - {}", res)
    }
  }

//  "The cluster monitor actor" should {
//    "connect to the cluster" in {
//      val monitor = TestFSMRef(new ClusterMonitorActor(hosts, 9042, settings))
//      monitor ! SubscribeTransitionCallBack(self)
//      within(60 seconds) {
//        expectMsgType[CurrentState[ClusterMonitorActor.State]]
//        val transition = expectMsgType[Transition[ClusterMonitorActor.State]]
//        assert(transition.to == Connected)
//      }
//    }
//  }


}
