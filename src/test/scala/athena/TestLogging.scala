package athena

import akka.testkit.TestKit
import akka.event.Logging

trait TestLogging { self: TestKit =>
  protected val log = Logging(system, self.getClass)
}
