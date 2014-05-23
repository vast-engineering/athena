package athena.connector

import org.scalatest.{Matchers, WordSpecLike}
import scala.language.postfixOps

import athena.Requests.SimpleStatement
import athena.Responses.Rows
import athena.AthenaTest
import athena.data.CValue


class NodeConnectorSpec extends AthenaTest with WordSpecLike with Matchers {

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
          row.zip(columnDefs).foreach { zipped =>
            val columnDef = zipped._2
            val value = CValue.parse(columnDef.dataType, zipped._1)
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
          row.zip(columnDefs).foreach { zipped =>
            val columnDef = zipped._2
            val value = CValue.parse(columnDef.dataType, zipped._1)
          }
        }
      }
    }

  }
}