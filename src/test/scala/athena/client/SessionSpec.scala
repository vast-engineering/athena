package athena.client

import org.scalatest.{WordSpec, Matchers}
import play.api.libs.iteratee.Iteratee
import scala.concurrent.{Await, Future}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import athena.TestData._
import akka.actor.ActorSystem
import akka.event.Logging

class SessionSpec extends WordSpec with Matchers {

  "A Session" should {
    "execute a query" in {

      implicit val system = ActorSystem("test-system")
      val log = Logging.getLogger(system, this.getClass)

      val session = Session(Hosts, Port)

      try {
        val aggregateRows = Await.result(session.execute("select * from testks.users"), Duration(10, TimeUnit.SECONDS))
        assert(!aggregateRows.isEmpty)

        val rows = Await.result(session.executeStream("select * from testks.users") |>>> Iteratee.getChunks, Duration(10, TimeUnit.SECONDS))
        assert(!rows.isEmpty)
      } finally {
        session.close()
        system.shutdown()
      }

    }
  }

}