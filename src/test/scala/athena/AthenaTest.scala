package athena

import akka.testkit._
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.{ExecutionContext, Await, Future}
import org.scalatest.{BeforeAndAfterAll, Suite}
import akka.io.IO
import java.net.{InetSocketAddress, InetAddress}

abstract class AthenaTest(_system: ActorSystem) extends TestKit(_system)
  with Suite with DefaultTimeout with ImplicitSender with BeforeAndAfterAll {

  def this() = this(ActorSystem("test-system"))

  implicit lazy val ec: ExecutionContext = system.dispatcher

  //ensure the actor actorRefFactory is shut down no matter what
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true

  import collection.JavaConversions._

  protected def await[T](f: Future[T]): T = Await.result(f, timeout.duration)

  import scala.concurrent.duration._
  import scala.language.postfixOps

  protected val hosts: Set[InetAddress] = system.settings.config.getStringList("athena.test.hosts").map(InetAddress.getByName)(collection.breakOut)
  protected val port = system.settings.config.getInt("athena.test.port")

  override protected def afterAll() {
    shutdown(system, verifySystemShutdown = true)
  }

  protected def withKeyspace[A](ksName: String)(f: => A): A = {
    //TODO: Add bits that create and drop a keyspace
    try {
      f
    } finally {
    }
  }

  protected def withClusterConnection[A](ksName: String = "testks")(f: ActorRef => A): A = {
    withKeyspace(ksName) {
      val probe = TestProbe()
      val connector = {
        IO(Athena).tell(Athena.ClusterConnectorSetup(hosts, port, None, useExisting = false), probe.ref)
        probe.expectMsgType[Athena.ClusterConnectorInfo]
        probe.fishForMessage() {
          case Athena.ClusterConnected => true
          case x: Athena.ClusterStatusEvent => false
        }
        probe.lastSender
      }

      try {
        f(connector)
      } finally {
        connector ! Athena.Close
        within(10 seconds) { expectMsgType[Athena.ConnectionClosed] }
      }
    }
  }

  protected def withNodeConnection[A](ksName: String = "testks")(f: ActorRef => A): A = {
    withKeyspace(ksName) {
      val connector = {
        IO(Athena) ! Athena.NodeConnectorSetup(hosts.head.getHostName, port, None)
        expectMsgType[Athena.NodeConnectorInfo].nodeConnector
        expectMsgType[Athena.NodeConnected]
        lastSender
      }

      try {
        f(connector)
      } finally {
        connector ! Athena.Close
        within(10 seconds) {
          expectMsgType[Athena.ConnectionClosed]
        }
      }
    }
  }

  protected def withConnection[A](keyspace: Option[String] = None)(f: ActorRef => A): A = {
    withKeyspace(keyspace.getOrElse("testks")) {
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
        connector ! Athena.Close
        within(10 seconds) { expectMsgType[Athena.ConnectionClosed] }
      }
    }
  }

}
