package athena.connector

import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import akka.actor.{ActorRef, ActorSystem}
import org.scalatest.{WordSpec, Matchers, WordSpecLike, BeforeAndAfterAll}
import akka.io.IO

import scala.concurrent.duration._
import scala.language.postfixOps

import athena.Requests.SimpleStatement
import athena.Responses.Rows
import athena.{AthenaTest, Athena}
import athena.data.CValue

import athena.TestData._

class NodeConnectorSpec extends WordSpec with AthenaTest with Matchers {

  val hostAddress = Hosts.head.getHostAddress

  private def openConnection(keyspace: Option[String] = None): ActorRef = {
    within(10 seconds) {
      IO(Athena) ! Athena.NodeConnectorSetup(hostAddress, Port, keyspace, None)
      expectMsgType[Athena.NodeConnectorInfo]
      val connection = lastSender
      expectMsgType[Athena.NodeConnected]
      connection
    }
  }

  "A NodeConnector" should {
    "start up properly" in {
      openConnection()
    }

    "execute a query" in {
      val connector = openConnection()

      val request = SimpleStatement("select * from testks.users")
      connector ! request
      val rows = expectMsgType[Rows]
      val columnDefs = rows.columnDefs

      rows.data.foreach { row =>
        log.debug("Row - ")
        row.zip(columnDefs).foreach { zipped =>
          val columnDef = zipped._2
          val value = CValue.parse(columnDef.dataType, zipped._1)
          log.debug(s"   ${columnDef.name} - ${columnDef.dataType.name} - $value")
        }
      }
    }

    "use an explicit keyspace" in {
      val connector = openConnection(Some("testks"))
      val request = SimpleStatement("select * from users")
      connector ! request
      val rows = expectMsgType[Rows]
      val columnDefs = rows.columnDefs

      rows.data.foreach { row =>
        log.debug("Row - ")
        row.zip(columnDefs).foreach { zipped =>
          val columnDef = zipped._2
          val value = CValue.parse(columnDef.dataType, zipped._1)
          log.debug(s"   ${columnDef.name} - ${columnDef.dataType.name} - $value")
        }
      }
    }

  }
}