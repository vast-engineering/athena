package athena

import java.net.InetSocketAddress
import athena.Athena._
import athena.data.Consistency._
import athena.util.MD5Digest
import athena.connector.CassandraError

object Errors {

  //signals something went wrong during communication with the server
  sealed trait GeneralError extends CassandraError
  case class ServerError(message: String, host: InetSocketAddress) extends GeneralError {
    def toThrowable: Throwable = new InternalException(s"An unexpected error occured server side on $host: $message")
  }
  case class ProtocolError(message: String) extends GeneralError {
    def toThrowable: Throwable = new InternalException(s"An unexpected protocol error occured. This is a bug in this library, please report: $message")
  }
  case class CredentialsError(message: String, host: InetSocketAddress) extends GeneralError {
    def toThrowable: Throwable = new AuthenticationException(message, host)
  }

  sealed trait RequestExecutionError extends CassandraError
  case class UnavailableError(message: String, consistency: Consistency, required: Int, alive: Int) extends RequestExecutionError {
    def toThrowable: Throwable = new UnavailableException(message, consistency, required, alive)
  }
  case class OverloadedError(message: String, host: InetSocketAddress) extends RequestExecutionError {
    def toThrowable: Throwable = new InternalException(s"Queried host ($host) was overloaded; this shouldn't happen, another node should have been tried")
  }
  case class IsBootstrappingError(message: String, host: InetSocketAddress) extends RequestExecutionError {
    def toThrowable: Throwable = new InternalException(s"Queried host ($host) was boostrapping; this shouldn't happen, another node should have been tried)")
  }
  case class TruncateError(message: String) extends RequestExecutionError {
    def toThrowable: Throwable = new TruncateException(message)
  }
  case class WriteTimeoutError(message: String, consistency: Consistency, received: Int, required: Int, writeType: String) extends RequestExecutionError {
    def toThrowable: Throwable = new WriteTimeoutException(message, consistency, writeType, received, required)
  }
  case class ReadTimeoutError(message: String, consistency: Consistency, received: Int, required: Int, dataPresent: Boolean) extends RequestExecutionError {
    def toThrowable: Throwable = new ReadTimeoutException(message, consistency, received, required, dataPresent)
  }

  sealed trait InvalidRequestError extends CassandraError
  case class SyntaxError(message: String) extends InvalidRequestError {
    def toThrowable: Throwable = new InvalidQuerySyntaxException(message)
  }
  case class UnauthorizedError(message: String) extends InvalidRequestError {
    def toThrowable: Throwable = new UnauthorizedException(message)
  }
  case class InvalidError(message: String) extends InvalidRequestError {
    def toThrowable: Throwable = new InvalidQueryException(message)
  }
  case class ConfigError(message: String) extends InvalidRequestError {
    def toThrowable: Throwable = new InvalidConfigurationInQueryException(message)
  }
  case class AlreadyExistsError(message: String, keyspace: String, table: String) extends InvalidRequestError {
    def toThrowable: Throwable = new AlreadyExistsException(message, keyspace, table)
  }
  case class UnpreparedError(message: String, host: InetSocketAddress, statementId: MD5Digest) extends InvalidRequestError {
    def toThrowable: Throwable = new InternalException(s"A prepared query was submitted on %host but was not known of that node; this shouldn't happen, the query should have been re-prepared")
  }

}