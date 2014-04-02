package athena.connector

import org.scalatest.{WordSpec, Matchers, WordSpecLike, BeforeAndAfterAll}

import scala.concurrent.duration._

import scala.language.postfixOps
import akka.io.IO
import athena.Requests.{FetchRows, SimpleStatement}
import athena.Responses.Rows
import athena.{AthenaTest, Athena}

import athena.TestData._

class ConnectionActorSpec extends WordSpec with AthenaTest with Matchers {

  val hostAddress = Hosts.head.getHostAddress

  "A ConnectionActor" when {
    "uninitialized" should {
      "start up properly" in {
        within(10 seconds) {
          IO(Athena) ! Athena.Connect(hostAddress, Port, None)
          expectMsgType[Athena.Connected]
        }
      }
    }

    "connected" should {
      val connectionActor = within(10 seconds) {
        IO(Athena) ! Athena.Connect(hostAddress, Port, None)
        expectMsgType[Athena.Connected]
        lastSender
      }

      "execute a query" in {
        within(10 seconds) {
          val request = SimpleStatement("select * from testks.users")
          connectionActor ! request
          val rows = expectMsgType[Rows]
          assert(rows.data.size == 3, "Expected three rows")
        }
      }

      "properly page results" in {
        within(10 seconds) {
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
      val keyspaceConnection = within(10 seconds) {
        IO(Athena) ! Athena.Connect(hostAddress, Port, Some("testks"))
        expectMsgType[Athena.Connected]
        lastSender
      }

      "execute a query" in {
        within(10 seconds) {
          val request = SimpleStatement("select * from users")
          keyspaceConnection ! request
          val rows = expectMsgType[Rows]
          assert(rows.data.size == 3, "Expected three rows")
        }
      }

      "properly page results" in {
        within(10 seconds) {
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
