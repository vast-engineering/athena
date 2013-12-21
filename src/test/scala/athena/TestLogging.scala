package athena

import akka.testkit.TestKit
import akka.event.Logging

trait TestLogging { self: TestKit =>
  val log = Logging(system, self.getClass)
}
