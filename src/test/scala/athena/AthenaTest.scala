package athena

import akka.testkit.{TestProbe, ImplicitSender, DefaultTimeout, TestKitBase}
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.{ExecutionContext, Await, Future}
import org.scalatest.{BeforeAndAfterAll, Suite}
import akka.event.Logging
import akka.io.IO
import java.net.InetAddress
import com.vast.farsandra.{ProcessManager, LineHandler, Farsandra}

trait TestLogging { self: TestKitBase =>
  val log = Logging(system, self.getClass)
}

trait AthenaTest extends TestKitBase with DefaultTimeout with ImplicitSender with BeforeAndAfterAll with TestLogging {
  self: Suite =>

  lazy val config: Config = ConfigFactory.load()
  implicit lazy val system: ActorSystem = ActorSystem("test-system", config)
  implicit lazy val ec: ExecutionContext = system.dispatcher

  //ensure the actor actorRefFactory is shut down no matter what
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true

  import collection.JavaConversions._

  private[this] var cassandraProcess: ProcessManager = _

  override protected def beforeAll() {
    val farsandra = new Farsandra()
      .withVersion("2.0.7")
      .withInstanceName(".farsandra-test")
      .withCleanInstanceOnStart(true)
      .withCreateConfigurationFiles(true)
      .withHost("localhost")
      .withSeeds(Seq("localhost"))
      .withOutputHandler(new LineHandler {
        override def handleLine(line: String) {
          log.debug(s"Cassandra out - $line")
        }
      })
      .withErrorHandler(new LineHandler {
        override def handleLine(line: String) {
          log.error(s"Cassandra err - $line")
        }
      })

    cassandraProcess = farsandra.start()

    val queryExecutor = farsandra.executeCQL("/schema.cql")
    if(queryExecutor.waitForShutdown(5000) != 0) {
      throw new RuntimeException("Could not execute CQL!")
    }
  }

  override protected def afterAll() {
    cassandraProcess.destroyAndWaitForShutdown(5000)
    shutdown(system, verifySystemShutdown = true)
  }

  protected def await[T](f: Future[T]): T = Await.result(f, timeout.duration)

  import scala.concurrent.duration._
  import scala.language.postfixOps

  protected val hosts = Set(InetAddress.getByName("localhost"))
  protected val port = 9042

  protected def withClusterConnection[A](keyspace: Option[String] = None)(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = within(10 seconds) {
      IO(Athena).tell(Athena.ClusterConnectorSetup(hosts, port, keyspace, None), probe.ref)
      probe.expectMsgType[Athena.ClusterConnectorInfo]
      probe.expectMsg(Athena.ClusterConnected)
      probe.lastSender
    }

    try {
      f(connector)
    } finally {
      akka.pattern.gracefulStop(connector, timeout.duration, Athena.Close)
    }
  }

  protected def withNodeConnection[A](keyspace: Option[String] = None)(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = within(10 seconds) {
      IO(Athena).tell(Athena.NodeConnectorSetup(hosts.head.getHostName, port, keyspace, None), probe.ref)
      probe.expectMsgType[Athena.NodeConnectorInfo].nodeConnector
      probe.expectMsgType[Athena.NodeConnected]
      probe.lastSender
    }

    try {
      f(connector)
    } finally {
      akka.pattern.gracefulStop(connector, timeout.duration, Athena.Close)
    }
  }

  protected def withConnection[A](keyspace: Option[String] = None)(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = within(10 seconds) {
      IO(Athena).tell(Athena.Connect(hosts.head.getHostName, port, keyspace), probe.ref)
      probe.expectMsgType[Athena.Connected]
      probe.lastSender
    }
    try {
      f(connector)
    } finally {
      akka.pattern.gracefulStop(connector, timeout.duration, Athena.Close)
    }

  }


}
