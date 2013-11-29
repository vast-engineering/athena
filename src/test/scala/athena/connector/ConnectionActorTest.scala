package athena.connector

import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import akka.actor.ActorSystem
import akka.testkit.DefaultTimeout
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import scala.concurrent.duration._

import scala.language.postfixOps
import akka.io.IO
import athena.Requests.SimpleStatement
import athena.Responses.Rows
import athena.{TestLogging, Athena}

class ConnectionActorTest extends TestKit(ActorSystem("test")) with TestLogging
with DefaultTimeout with ImplicitSender
with WordSpec with ShouldMatchers with BeforeAndAfterAll  {

  override def afterAll() {
    shutdown(system)
  }

  "A ConnectionActor" should {
    "start up properly" in {
      within(10 seconds) {
        IO(Athena) ! Athena.Connect("127.0.0.1", 9042, None)
        expectMsgType[Athena.Connected]
      }
    }

    "execute a query" in {
      val connectionActor = within(10 seconds) {
        IO(Athena) ! Athena.Connect("127.0.0.1", 9042, None)
        expectMsgType[Athena.Connected]
        lastSender
      }

      within(10 seconds) {
        val request = SimpleStatement("select * from testks.users")
        connectionActor ! request
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

    "use an explicit keyspace" in {
      val connectionActor = within(10 seconds) {
        IO(Athena) ! Athena.Connect("127.0.0.1", 9042, Some("testks"))
        expectMsgType[Athena.Connected]
        lastSender
      }

      within(10 seconds) {
        val request = SimpleStatement("select * from users")
        connectionActor ! request
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
  }

}
