package athena.connector

import akka.testkit._
import akka.actor.ActorSystem
import org.scalatest.{WordSpec, BeforeAndAfterAll, Matchers, WordSpecLike}
import athena.{AthenaTest, Athena}
import scala.concurrent.duration._
import akka.io.IO
import athena.client.pipelining
import scala.Some
import java.util.concurrent.TimeUnit
import akka.util.Timeout
import scala.concurrent.Await

import athena.TestData._

class ClusterUtilsSpec extends WordSpec with AthenaTest with Matchers with ClusterUtils {

  private[this] val timeoutDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  private[this] implicit val timeout = Timeout(timeoutDuration)

  val host = Hosts.head

  "The cluster utils" should {
    "get the cluster info" in {
      val pipeline = within(timeoutDuration) {
        IO(Athena) ! Athena.NodeConnectorSetup(host.getHostAddress, Port, Some("testks"), None)
        expectMsgType[Athena.NodeConnectorInfo]
        expectMsgType[Athena.NodeConnected]
        pipelining.queryPipeline(lastSender)
      }

      val res = Await.result(updateClusterInfo(host, pipeline), timeoutDuration)
      log.debug("Cluster info - {}", res)
    }
  }

}
