package athena.client

import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.ShouldMatchers
import akka.io.IO
import athena.{TestLogging, Athena}
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.Await

import scala.language.postfixOps
import java.util.concurrent.TimeUnit

class ConnectionTest extends TestKit(ActorSystem("test"))
with DefaultTimeout with ImplicitSender
with WordSpec with ShouldMatchers with BeforeAndAfterAll with TestLogging {

  private[this] val timeoutDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  private[this] implicit val timeout = Timeout(timeoutDuration)

  override def afterAll() {
    shutdown(system)
  }

  "A Connection" should {

    val connection = within(timeoutDuration) {
      IO(Athena) ! Athena.NodeConnectorSetup("127.0.0.1", 9042, Some("testks"), None)
      expectMsgType[Athena.NodeConnectorInfo]
      new Connection(lastSender)
    }

    "execute a query" in {
      val resultPage = Await.result(connection.execute("select * from users"), timeoutDuration)
      log.debug("Query results - ")
      resultPage.rows.foreach { row =>
        log.debug(s"${row.columns.mkString(", ")}")
      }
    }


  }

}
