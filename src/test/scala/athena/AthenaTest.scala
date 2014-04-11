package athena

import akka.testkit.{TestProbe, ImplicitSender, DefaultTimeout, TestKitBase}
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.{ExecutionContext, Await, Future}
import org.scalatest.{BeforeAndAfterAll, Suite}
import akka.event.Logging
import akka.io.IO
import java.net.{InetSocketAddress, InetAddress}
import athena.testutils.{ProcessManager, LineHandler, CassandraManager}
import scala.util.control.NonFatal

trait TestLogging { self: TestKitBase =>
  val log = Logging(system, self.getClass)
}

trait AthenaTest extends TestKitBase with DefaultTimeout with ImplicitSender with BeforeAndAfterAll with TestLogging {
  thisSuite: Suite =>

  lazy val config: Config = ConfigFactory.load()
  implicit lazy val system: ActorSystem = ActorSystem("test-system", config)
  implicit lazy val ec: ExecutionContext = system.dispatcher

  //ensure the actor actorRefFactory is shut down no matter what
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true

  import collection.JavaConversions._

  private[this] var cassandraProcess: Option[ProcessManager] = None

  override protected def beforeAll() {
    val cassandraManager = new CassandraManager()
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


    if(config.getBoolean("athena.test.start-cassandra")) {
      cassandraProcess = Some(cassandraManager.start())
    }

    if(config.getBoolean("athena.test.create-keyspace")) {
      val queryExecutor = cassandraManager.executeCQL("/schema.cql")
      if(queryExecutor.waitForShutdown(5000) != 0) {
        throw new RuntimeException("Could not execute CQL!")
      }
    }
  }

  override protected def afterAll() {
    cassandraProcess.foreach { cp =>
      if(cp.destroyAndWaitForShutdown(5000) != 0) {
        throw new RuntimeException("Could not stop cassandra!")
      }
    }
    shutdown(system, verifySystemShutdown = true)
  }

  protected def await[T](f: Future[T]): T = Await.result(f, timeout.duration)

  import scala.concurrent.duration._
  import scala.language.postfixOps

  protected val hosts: Set[InetAddress] = config.getStringList("athena.test.hosts").map(InetAddress.getByName)(collection.breakOut)
  protected val port = 9042

  protected def withClusterConnection[A]()(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = {
      IO(Athena).tell(Athena.ClusterConnectorSetup(hosts, port, None), probe.ref)
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

  protected def withNodeConnection[A]()(f: ActorRef => A): A = {
    val connector = {
      IO(Athena) ! Athena.NodeConnectorSetup(hosts.head.getHostName, port, None)
      expectMsgType[Athena.NodeConnectorInfo].nodeConnector
      expectMsgType[Athena.NodeConnected]
      lastSender
    }

    try {
      f(connector)
    } finally {
      akka.pattern.gracefulStop(connector, timeout.duration, Athena.Close)
    }
  }

  protected def withConnection[A](keyspace: Option[String] = None)(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = {
      IO(Athena).tell(Athena.Connect(new InetSocketAddress(hosts.head.getHostName, port), initialKeyspace = keyspace), probe.ref)
      probe.expectMsgType[Athena.Connected]
      val connection = probe.lastSender
      connection ! Athena.Register(self)
      connection
    }
    try {
      f(connector)
    } finally {
      akka.pattern.gracefulStop(connector, timeout.duration, Athena.Close)
    }

  }


}
