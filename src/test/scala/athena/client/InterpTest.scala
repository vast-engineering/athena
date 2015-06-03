package athena.client

import org.scalatest.WordSpecLike
import athena.AthenaTest
import play.api.libs.iteratee.Iteratee
import athena.data.CvResult

class InterpTest extends AthenaTest with WordSpecLike {

  import QueryInterpolation._

  "A query literal" should {
    "work properly" in {
      withKeyspace("testks") {
        implicit val session = Session(hosts, port, "testks")

        val id = 1234
        val rowMapper = Iteratee.getChunks[CvResult[(Long, String, String)]].map { result =>
          result.map(_.get)
        }
        await(cql"select id, first_name, last_name from users where id = $id".as[(Long, String, String)].execute.run(rowMapper))

        session.close()
      }
    }
  }

}

