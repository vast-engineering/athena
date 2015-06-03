package athena.client

import athena.AthenaTest
import org.scalatest.WordSpecLike

class RowReaderSpec extends AthenaTest with WordSpecLike {

  import QueryInterpolation._
  import RowReaderSpec._

  "RowReaders" should {
    "work with a default tuple instance" in withKeyspace("testks") {
      implicit val session = Session(hosts, port, "testks")

      val id = 1234
      val result = cql"select id, first_name, last_name from users where id = $id".as[(Long, String, String)].extractValues.fold(0) { (acc, row) =>
        acc + 1
      }
      await(result)

      session.close()
    }

    "work with a synthesized reader instance" in withKeyspace("testks") {
      implicit val session = Session(hosts, port, "testks")

      val id = 1234
      val result = cql"select id, first_name, last_name from users where id = $id".as[TestResult].extractValues.fold(0) { (acc, row) =>
        acc + 1
      }
      await(result)

      session.close()
    }
  }

}

object RowReaderSpec {
  import athena.client.RowReader._

  case class TestResult(id: Long, firstName: String, lastName: String)

  implicit val testResultReads: RowReader[TestResult] = (
    at[Long](0) ::
    at[String](1) ::
    at[String](2)
  )(TestResult.apply _)


}
