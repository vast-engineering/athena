package athena.connector

import org.scalatest.{Matchers, WordSpecLike}
import athena.AthenaTest
import scala.concurrent.duration._
import athena.client.pipelining
import java.util.concurrent.TimeUnit
import akka.util.Timeout
import scala.concurrent.Await
import akka.event.{Logging, LoggingAdapter}

class ClusterUtilsSpec extends AthenaTest with WordSpecLike with Matchers with ClusterUtils {

  private[this] val timeoutDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  private[this] implicit val timeout = Timeout(timeoutDuration)

  override lazy val log: LoggingAdapter = Logging(system, this.getClass)

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
