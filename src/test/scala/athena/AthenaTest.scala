package athena

import akka.testkit.{TestProbe, ImplicitSender, DefaultTimeout, TestKitBase}
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.{ExecutionContext, Await, Future}
import org.scalatest.{BeforeAndAfterAll, Suite}
import akka.event.Logging
import akka.io.IO
import athena.TestData._
import java.net.InetAddress

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

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
  }

  protected def await[T](f: Future[T]): T = Await.result(f, timeout.duration)

  import scala.concurrent.duration._
  import scala.language.postfixOps

  protected def withClusterConnection[A](hosts: Set[InetAddress], keyspace: Option[String] = None)(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = within(10 seconds) {
      IO(Athena).tell(Athena.ClusterConnectorSetup(hosts, Port, keyspace, None), probe.ref)
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

  protected def withNodeConnection[A](host: String, keyspace: Option[String] = None)(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = within(10 seconds) {
      IO(Athena).tell(Athena.NodeConnectorSetup(host, Port, keyspace, None), probe.ref)
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

  protected def withConnection[A](host: String, keyspace: Option[String] = None)(f: ActorRef => A): A = {
    val probe = TestProbe()
    val connector = within(10 seconds) {
      IO(Athena).tell(Athena.Connect(host, Port, keyspace), probe.ref)
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
