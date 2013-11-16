package com.vast.verona.client

import akka.util.ByteString
import com.vast.verona.util.MD5Digest
import com.vast.verona.data.Consistency.Consistency
import com.vast.verona.data.Metadata

sealed trait Response

case object Ready extends Response

sealed trait Error extends Response {
  def message: String
}

//signals something went wrong during communication with the server
sealed trait GeneralError extends Error
case class ServerError(message: String) extends GeneralError
case class ProtocolError(message: String) extends GeneralError
case class CredentialsError(message: String) extends GeneralError

sealed trait RequestExecutionError extends Error
case class UnavailableError(message: String, consistency: Consistency, required: Int, alive: Int) extends RequestExecutionError
case class OverloadedError(message: String) extends RequestExecutionError
case class IsBootstrappingError(message: String) extends RequestExecutionError
case class TruncateError(message: String) extends RequestExecutionError
case class WriteTimeoutError(message: String, consistency: Consistency, received: Int, blockFor: Int, writeType: String) extends RequestExecutionError
case class ReadTimeoutError(message: String, consistency: Consistency, received: Int, blockFor: Int, dataPresent: Boolean) extends RequestExecutionError

sealed trait InvalidRequestError extends Error
case class SyntaxError(message: String) extends InvalidRequestError
case class UnauthorizedError(message: String) extends InvalidRequestError
case class InvalidError(message: String) extends InvalidRequestError
case class ConfigError(message: String) extends InvalidRequestError
case class AlreadyExistsError(message: String, keyspace: String, table: String) extends InvalidRequestError
case class UnpreparedError(message: String, statementId: MD5Digest) extends InvalidRequestError


sealed abstract class Result extends Response

case object Successful extends Result
case class Rows(metadata: Metadata, data: Seq[Seq[ByteString]]) extends Result
