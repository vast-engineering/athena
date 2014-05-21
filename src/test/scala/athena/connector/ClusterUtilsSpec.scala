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
import akka.event.{Logging, LoggingAdapter}

class ClusterUtilsSpec extends WordSpec with AthenaTest with Matchers with ClusterUtils {

  private[this] val timeoutDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  private[this] implicit val timeout = Timeout(timeoutDuration)


  override def log: LoggingAdapter = Logging(system, this.getClass)

  val host = hosts.head

  "The cluster utils" should {
    "get the cluster info" in {
      withClusterConnection() { connector =>
        val pipeline = pipelining.queryPipeline(connector)
        val res = Await.result(updateClusterInfo(host, pipeline), timeoutDuration)
        log.debug("Cluster info - {}", res)
      }
    }
  }

}
