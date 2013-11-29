package athena.connector

import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.ShouldMatchers
import akka.io.IO

import scala.concurrent.duration._

import scala.language.postfixOps
import athena.Requests.SimpleStatement
import athena.Responses.Rows
import athena.{TestLogging, Athena}

class NodeConnectorTest extends TestKit(ActorSystem("test"))
with DefaultTimeout with ImplicitSender
with WordSpec with ShouldMatchers with BeforeAndAfterAll with TestLogging {

  override def afterAll() {
    shutdown(system)
  }

  "A NodeConnector" should {
    "start up properly" in {
      within(10 seconds) {
        IO(Athena) ! Athena.NodeConnectorSetup("127.0.0.1", 9042, None, None)
        expectMsgType[Athena.NodeConnectorInfo]
        lastSender
      }
    }
  }

  "execute a query" in {
    val connector = within(10 seconds) {
      IO(Athena) ! Athena.NodeConnectorSetup("127.0.0.1", 9042, None, None)
      expectMsgType[Athena.NodeConnectorInfo]
      lastSender
    }

    val request = SimpleStatement("select * from testks.users")
    connector ! request
    val rows = expectMsgType[Rows]
    val columnDefs = rows.columnDefs

    rows.data.foreach { row =>
      log.debug("Row - ")
      row.zip(columnDefs).foreach { zipped =>
        val columnDef = zipped._2
        val value = columnDef.dataType.decode(zipped._1)
        log.debug(s"   ${columnDef.name} - ${columnDef.dataType.name} - $value")
      }
    }
  }

  "use an explicit keyspace" in {
    val connector = within(10 seconds) {
      IO(Athena) ! Athena.NodeConnectorSetup("127.0.0.1", 9042, Some("testks"), None)
      expectMsgType[Athena.NodeConnectorInfo]
      lastSender
    }

    val request = SimpleStatement("select * from users")
    connector ! request
    val rows = expectMsgType[Rows]
    val columnDefs = rows.columnDefs

    rows.data.foreach { row =>
      log.debug("Row - ")
      row.zip(columnDefs).foreach { zipped =>
        val columnDef = zipped._2
        val value = columnDef.dataType.decode(zipped._1)
        log.debug(s"   ${columnDef.name} - ${columnDef.dataType.name} - $value")
      }
    }
  }
}