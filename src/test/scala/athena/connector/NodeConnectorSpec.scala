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


class NodeConnectorSpec extends WordSpec with AthenaTest with Matchers {

  val hostAddress = hosts.head.getHostAddress

  "A NodeConnector" should {
    "start up properly" in {
      withNodeConnection() { connector =>
      }
    }

    "execute a query" in {
      withNodeConnection() { connector =>
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
    }

    "use an explicit keyspace" in {
      withNodeConnection() { connector =>
        val request = SimpleStatement("select * from users", keyspace = Some("testks"))
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
}