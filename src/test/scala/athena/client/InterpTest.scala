package athena.client

import org.scalatest.WordSpec
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import athena.AthenaTest

class InterpTest extends WordSpec with AthenaTest {

  import QueryInterpolation._

  import RowReader._

  case class TestRes(a: String, b: String)

  "A query literal" should {

    "work properly" in {
      implicit val session = Session(hosts, port, "testks")
      log.info("Starting query.")

      val id = 1234
      val foo = await(cql"select * from users where id = $id".as[(Long, String)].execute.run(Iteratee.getChunks))

      log.info("Finished query with result {}", foo)
      session.close()
    }
  }

}
