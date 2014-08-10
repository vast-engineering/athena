package athena

import akka.testkit._
import akka.actor.{Actor, Props, ActorRef, ActorSystem}
import athena.connector.{ClusterConnector, ConnectionActor, NodeConnector}
import scala.concurrent.{ExecutionContext, Await, Future}
import org.scalatest.{BeforeAndAfterAll, Suite}
import java.net.{InetSocketAddress, InetAddress}

import scala.util.Random

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
  protected val config = system.settings.config

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
      proxied(ClusterConnector.props(hosts, port, ClusterConnectorSettings(system)), "cluster-connector") { proxy =>
        fishForMessage() {
          case Athena.ClusterConnected => true
          case x: Athena.ClusterStatusEvent => false
        }
        try {
          f(proxy)
        } finally {
          proxy ! Athena.Close
          within(10 seconds) { expectMsgType[Athena.ConnectionClosed] }
        }
      }
    }
  }

  protected def withNodeConnection[A](ksName: String = "testks")(f: ActorRef => A): A = {
    withKeyspace(ksName) {
      val addr = new InetSocketAddress(hosts.head.getHostName, port)
      val settings = NodeConnectorSettings(system)
      proxied(NodeConnector.props(addr, settings), "node-connection") { proxy =>
        expectMsgType[Athena.NodeConnected]
        try {
          f(proxy)
        } finally {
          proxy ! Athena.Close
          within(10 seconds) {
            expectMsgType[Athena.ConnectionClosed]
          }
        }
      }
    }
  }

  protected def withConnection[A](keyspace: Option[String] = None)(f: ActorRef => A): A = {
    withKeyspace(keyspace.getOrElse("testks")) {
      val addr = new InetSocketAddress(hosts.head.getHostName, port)
      val settings = ConnectionSettings(system)
      proxied(ConnectionActor.props(addr, settings, keyspace), "connection") { proxy =>
        expectMsgType[Athena.Connected]
        try {
          f(proxy)
        } finally {
          proxy ! Athena.Close
          within(10 seconds) { expectMsgType[Athena.ConnectionClosed] }
        }
      }
    }
  }

  private def proxied[A](props: Props, name: String)(f: ActorRef => A): A = {
      val proxy = TestActorRef(props, testActor, s"name-${Random.nextInt()}")
//    val parent = TestProbe()
//    val proxy = system.actorOf(Props(new Actor {
//      val addr = new InetSocketAddress(hosts.head.getHostName, port)
//      val settings = NodeConnectorSettings(system)
//      val child = context.actorOf(props, name)
//      def receive = {
//        case x if sender == child => parent.ref forward x
//        case x => child forward x
//      }
//    }))
    try {
      f(proxy)
    } finally {
      system.stop(proxy)
    }
  }


}
