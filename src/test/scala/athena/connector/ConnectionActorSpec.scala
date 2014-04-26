package athena.connector

import org.scalatest.{WordSpec, Matchers, WordSpecLike, BeforeAndAfterAll}

import scala.concurrent.duration._

import scala.language.postfixOps
import athena.Requests.{FetchRows, SimpleStatement}
import athena.Responses.Rows
import athena.{AthenaTest, Athena}


class ConnectionActorSpec extends WordSpec with AthenaTest with Matchers {

  "A ConnectionActor" when {
    "uninitialized" should {
      "start up properly" in {
        withConnection() { connection =>

        }
      }
    }

    "connected" should {
      "execute a query" in {
        withConnection() { connectionActor =>
          val request = SimpleStatement("select * from testks.users")
          connectionActor ! request
          val rows = expectMsgType[Rows]
          assert(rows.data.size == 3, "Expected three rows")
        }
      }

      "properly page results" in {
        withConnection() { connectionActor =>
          val request = SimpleStatement("select * from testks.users", fetchSize = Some(1))
          connectionActor ! request
          val rows = expectMsgType[Rows]
          assert(rows.data.size == 1, "Expected one row")

          connectionActor ! FetchRows(rows.request, rows.pagingState.get)
          val rows2 = expectMsgType[Rows]
          assert(rows2.data.size == 1, "Expected one row")
        }
      }

    }

    "connected with a keyspace" should {
      "execute a query" in {
        withConnection(Some("testks")) { keyspaceConnection =>
          val request = SimpleStatement("select * from users")
          keyspaceConnection ! request
          val rows = expectMsgType[Rows]
          assert(rows.data.size == 3, "Expected three rows")
        }
      }

      "properly page results" in {
        withConnection(Some("testks")) { keyspaceConnection =>
          val request = SimpleStatement("select * from users", fetchSize = Some(1))
          keyspaceConnection ! request
          val rows = expectMsgType[Rows]
          assert(rows.data.size == 1, "Expected one row")

          keyspaceConnection ! FetchRows(rows.request, rows.pagingState.get)
          val rows2 = expectMsgType[Rows]
          assert(rows2.data.size == 1, "Expected one row")
        }
      }

    }
  }

}
