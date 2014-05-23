package athena.connector

import org.scalatest.{WordSpecLike, Matchers}
import athena.AthenaTest
import athena.Requests.SimpleStatement
import athena.Responses.Rows

import scala.language.postfixOps


class ClusterConnectorSpec extends AthenaTest with WordSpecLike with Matchers {

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
        assert(rows.data.size === 3)
      }
    }

    "use an explicit keyspace" in {
      withClusterConnection() { connector =>

        val request = SimpleStatement("select * from users", keyspace = Some("testks"))
        connector ! request
        val rows = expectMsgType[Rows]
        val columnDefs = rows.columnDefs
        assert(rows.data.size === 3)
      }
    }

  }
}