package athena.client

import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import akka.actor.{ActorSystem}
import org.scalatest.{Matchers, WordSpecLike, BeforeAndAfterAll}
import akka.io.IO
import athena.{TestLogging, Athena}
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.Await

import scala.language.postfixOps
import java.util.concurrent.TimeUnit
import athena.Requests.{SimpleStatement, Statement}
import play.api.libs.iteratee.{Iteratee, Enumerator}

import athena.TestData._

class PipelineSpec extends TestKit(ActorSystem("test")) with WordSpecLike
with DefaultTimeout with ImplicitSender
with Matchers with BeforeAndAfterAll with TestLogging {

  private[this] val timeoutDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  private[this] implicit val timeout = Timeout(timeoutDuration)

  override def afterAll() {
    shutdown(system)
  }

  import system.dispatcher

  "A pipeline" when {
    "using a raw connection" should {
      val pipeline = within(timeoutDuration) {
        IO(Athena) ! Athena.Connect(Hosts.head.getHostAddress, Port, Some("testks"), None)
        expectMsgType[Athena.Connected]
        Pipelining.queryPipeline(lastSender)
      }
      "execute a simple query" in {
        simpleQuery(pipeline)
      }
    }
    "using a node connection pool" should {
      val connection = within(timeoutDuration) {
        IO(Athena) ! Athena.NodeConnectorSetup(Hosts.head.getHostAddress, Port, Some("testks"), None)
        expectMsgType[Athena.NodeConnectorInfo]
        Pipelining.queryPipeline(lastSender)
      }
      "execute a simple query" in {
        simpleQuery(connection)
      }
    }
  }

  private def simpleQuery(pipeline: Statement => Enumerator[Row]) {
    val results = Await.result(pipeline(SimpleStatement("select * from users")).run(Iteratee.getChunks), timeoutDuration)
    log.debug("Query results - ")
    results.foreach { row =>
        log.debug(s"${row.values.mkString(", ")}")
    }
  }


}
