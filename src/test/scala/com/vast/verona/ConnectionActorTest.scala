package com.vast.verona


import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.DefaultTimeout
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import scala.concurrent.duration._

import scala.language.postfixOps
import java.nio.ByteOrder
import com.vast.verona.client._
import com.vast.verona.client.Startup
import com.vast.verona.client.Rows
import com.vast.verona.client.Ready
import com.vast.verona.client.Query
import com.vast.verona.data.Consistency
import akka.io.IO

class ConnectionActorTest extends TestKit(ActorSystem("test"))
with DefaultTimeout with ImplicitSender
with WordSpec with ShouldMatchers with BeforeAndAfterAll {

  implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  override def afterAll() {
    shutdown(system)
  }

  "A ConnectionActor" should {
    "start up properly" in {
      within(10 seconds) {
        IO(Cassandra) ! Cassandra.Connect("cassandra1.oak.vast.com", 9042, None)
        val ready = expectMsgType[Cassandra.Connected]
        Console.println(s"Got response - $ready")
      }
    }

    "execute a query" in {
      val connectionActor = within(10 seconds) {
        IO(Cassandra) ! Cassandra.Connect("cassandra1.oak.vast.com", 9042, None)
        val ready = expectMsgType[Cassandra.Connected]
        Console.println(s"Got response - $ready")
        lastSender
      }

      within(5 seconds) {
        connectionActor ! Query("select * from vastre.inventory limit 10", Consistency.ONE)
        val rows = expectMsgType[Rows]
        val columnDefs = rows.metadata.columns.get

        rows.data.foreach { row =>
          Console.println("Row - ")
          row.zip(columnDefs).foreach { zipped =>
            val columnDef = zipped._2
            val value = columnDef.dataType.decode(zipped._1)
            Console.println(s"   ${columnDef.name} - ${columnDef.dataType.name} - $value")
          }
        }

        Console.println(rows)
      }
    }
  }

}
