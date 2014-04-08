package athena.client

import org.scalatest.WordSpec
import athena.TestData._
import akka.actor.ActorSystem
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.event.Logging
import athena.AthenaTest

class InterpTest extends WordSpec with AthenaTest {

  import QueryInterpolation._

  import RowReader._

  case class TestRes(a: String, b: String)

  "A query literal" should {

    "work properly" in {
      implicit val session = Session(Hosts, Port)
      log.info("Starting query.")

      val id = 1745
      val foo = Await.result(
        cql"select * from testks.users where user_id = $id".as[(Long, String)].execute.run(Iteratee.getChunks),
        Duration.Inf
      )

      log.info("Finished query with result {}", foo)
      session.close()
    }
  }

}
