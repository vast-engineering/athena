package athena

import akka.testkit.{ImplicitSender, DefaultTimeout, TestKitBase}
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Await, Future}
import org.scalatest.{BeforeAndAfterAll, Suite}
import akka.event.Logging

trait TestLogging { self: TestKitBase =>
  val log = Logging(system, self.getClass)
}

trait AthenaTest extends TestKitBase with DefaultTimeout with ImplicitSender with BeforeAndAfterAll with TestLogging {
  self: Suite =>

  lazy val config: Config = ConfigFactory.load()
  implicit lazy val system: ActorSystem = ActorSystem("hs-test-actorRefFactory", config)
  implicit lazy val ec: ExecutionContext = system.dispatcher

  //ensure the actor actorRefFactory is shut down no matter what
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
  }

  protected def await[T](f: Future[T]): T = Await.result(f, timeout.duration)

}
