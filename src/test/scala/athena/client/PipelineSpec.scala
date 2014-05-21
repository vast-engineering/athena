package athena.client

import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import akka.actor.{ActorSystem}
import org.scalatest.{WordSpec, Matchers, WordSpecLike, BeforeAndAfterAll}
import akka.io.IO
import athena.{AthenaTest, Athena}
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.Await

import scala.language.postfixOps
import java.util.concurrent.TimeUnit
import athena.Requests.{SimpleStatement, Statement}
import play.api.libs.iteratee.{Iteratee, Enumerator}


class PipelineSpec extends WordSpec with AthenaTest with Matchers {

  private[this] val timeoutDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)

  "A pipeline" when {
    "using a raw connection" should {
      "execute a simple query" in {
        withConnection(Some("testks")) { connection =>
          simpleQuery(pipelining.queryPipeline(connection))
        }

      }
    }
    "using a node connection pool" should {
      "execute a simple query" in {
        withNodeConnection() { connection =>
          simpleQuery(pipelining.queryPipeline(connection))
        }
      }
    }
    "using a cluster connection" should {
      "execute a simple query" in {
        withClusterConnection() { connection =>
          simpleQuery(pipelining.queryPipeline(connection))
        }
      }
    }
  }

  private def simpleQuery(pipeline: Statement => Enumerator[Row]) {
    val results = Await.result(pipeline(SimpleStatement("select * from users", keyspace = Some("testks"))).run(Iteratee.getChunks), timeoutDuration)
    testLogger.debug("Query results - ")
    results.foreach { row =>
      testLogger.debug(s"${row.values.mkString(", ")}")
    }
    assert(!results.isEmpty)
  }


}
