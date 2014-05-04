package athena.connector

import org.scalatest.{WordSpec, Matchers, WordSpecLike, BeforeAndAfterAll}

import scala.concurrent.duration._

import scala.language.postfixOps
import athena.Requests.{BoundStatement, Prepare, FetchRows, SimpleStatement}
import athena.Responses.{ErrorResponse, Prepared, Rows}
import athena.AthenaTest
import athena.data.CInt
import athena.connector.ConnectionActor.{ConnectionCommandFailed, SetKeyspace}
import athena.Errors.{SyntaxError, InvalidError}


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

      "reject an invalid keyspace change request" in {
        withConnection() { connectionActor =>
          connectionActor ! SetKeyspace("bad_keyspace1234")
          val responseMessage = expectMsgPF() {
            case ConnectionCommandFailed(_, Some(InvalidError(message))) => message
          }
          log.debug("Got response - {}", responseMessage)
        }
      }

      "reject a USE statement" in {
        withConnection() { connectionActor =>
          val request = SimpleStatement("  uSe testks   ")
          connectionActor ! request
          val responseMessage = expectMsgPF() {
            case ErrorResponse(_, SyntaxError(message)) => message
          }
          log.debug("Got response - {}", responseMessage)
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

      "prepare a statement" in {
        withConnection(Some("testks")) { keyspaceConnection =>
          val request = Prepare("select * from users where id = ?", Some("testks"))
          keyspaceConnection ! request
          val prepared = expectMsgType[Prepared]
          assert(prepared.request == request, "Requests did not match up.")

          val bound = BoundStatement(prepared.statementDef, Seq(CInt(1234)))
          keyspaceConnection ! bound
          val rows = expectMsgType[Rows]
          assert(rows.data.size == 1, "Expected one row")
        }
      }

    }
  }

}
