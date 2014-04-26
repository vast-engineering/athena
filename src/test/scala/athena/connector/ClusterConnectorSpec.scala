package athena.connector

import akka.actor.ActorRef
import org.scalatest.{WordSpec, Matchers}
import athena.{AthenaTest, Athena}
import akka.io.IO
import athena.Requests.SimpleStatement
import athena.Responses.Rows
import athena.data.CValue

import scala.concurrent.duration._
import scala.language.postfixOps


class ClusterConnectorSpec extends WordSpec with AthenaTest with Matchers {

  "A Cluster connector" should {
    "start up properly" in {
      withClusterConnection() { c =>
      }
    }

    "execute a query" in {
      withClusterConnection() { connector =>
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
      withClusterConnection(Some("testks")) { connector =>

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
}