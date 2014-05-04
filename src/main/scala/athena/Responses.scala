package athena

import akka.util.ByteString
import athena.Requests.{Prepare, AthenaRequest, Statement}
import athena.data.{PreparedStatementDef, ColumnDef}
import scala.Predef._
import java.net.InetAddress


object Responses {

  sealed trait AthenaResponse {
    def request: AthenaRequest
    def isFailure: Boolean
  }

  sealed trait FailureResponse extends AthenaResponse {
    def isFailure: Boolean = true
  }

  /**
   * Signals that a request failed to generate a response from the server for any reason.
   */
  case class RequestFailed(request: AthenaRequest) extends FailureResponse

  /**
   * Sent when a request is submitted to an actor in an invalid state. For example, a query with an explicit
   * keyspace set cannot be sent to a connection that is currently set to a different keyspace.
   */
  case class InvalidRequest(request: AthenaRequest) extends FailureResponse

  /**
   * Signals that a request timed out.
   */
  case class Timedout(request: AthenaRequest) extends FailureResponse

  /**
   * Signals that a request was unable to execute on any of the hosts in the cluster.
   */
  case class NoHostsAvailable(request: AthenaRequest, errors: Map[InetAddress, AthenaResponse]) extends FailureResponse

  /**
   * Signals that a request could not be sent due to a connection problem
   */
  case class ConnectionUnavailable(request: AthenaRequest) extends FailureResponse

  sealed abstract class SuccessfulResponse extends AthenaResponse {
    def isFailure: Boolean = false
  }

  /**
   * A response indicating that the request itself was unsuccessful - this is opposed to a [[athena.Responses.FailureResponse]]
   * which indicates a problem during sending the request or receiving the response.
   *
   * This class intentionally inherits from [[athena.Responses.SuccessfulResponse]] due to the fact that the response itself
   * was actually sucessfully generated - it just so happens that the contents of the response indicate an error.
   */
  case class ErrorResponse(request: AthenaRequest, error: Athena.Error) extends SuccessfulResponse

  sealed trait QueryResult extends SuccessfulResponse

  /**
   * Used to indicate a result-less successful query.
   */
  case class Successful(request: AthenaRequest) extends QueryResult

  //this will be the response (if successful) from a Statement command
  case class Rows(request: Statement, columnDefs: IndexedSeq[ColumnDef],
                  data: Seq[IndexedSeq[ByteString]], pagingState: Option[ByteString]) extends QueryResult

  case class Prepared(request: Prepare, statementDef: PreparedStatementDef) extends SuccessfulResponse


}

