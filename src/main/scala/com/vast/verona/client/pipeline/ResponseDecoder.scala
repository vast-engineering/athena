package com.vast.verona.client.pipeline

import com.vast.verona._
import akka.util.ByteString
import java.nio.ByteOrder
import javax.naming.OperationNotSupportedException
import com.vast.verona.util.MD5Digest
import com.vast.verona.client._
import com.vast.verona.client.Ready

private[pipeline] object ResponseDecoder {

  import ResponseOpcodes._

  def decode(opcode: ResponseOpcode, bs: ByteString)(implicit byteOrder: ByteOrder): Response = {
    opcode match {
      case READY => Ready
      case RESULT => decodeResult(bs)
      case ERROR => decodeError(bs)
      case x => notImplemented(x)
      //      case ERROR =>
      //      case AUTHENTICATE =>
      //      case SUPPORTED =>
      //      case EVENT =>
      //      case AUTH_CHALLENGE =>
      //      case AUTH_SUCCESS =>
    }
  }

  private def notImplemented(opcode: ResponseOpcode) = throw new UnsupportedOperationException(s"Response parser for $opcode is not implemented.")

  private def decodeError(bs: ByteString)(implicit byteOrder: ByteOrder): client.Error = {
    val it = bs.iterator
    val errorCode = it.getInt
    val message = ByteStringUtils.readString(it)

    import ErrorCodes._

    errorCode match {
      case SERVER_ERROR => ServerError(message)
      case PROTOCOL_ERROR => ProtocolError(message)
      case BAD_CREDENTIALS => CredentialsError(message)

      case UNAVAILABLE =>
        val consistency = ByteStringUtils.readConsistency(it)
        val required = it.getInt
        val alive = it.getInt
        UnavailableError(message, consistency, required, alive)
      case OVERLOADED => OverloadedError(message)
      case IS_BOOTSTRAPPING => IsBootstrappingError(message)
      case TRUNCATE_ERROR => TruncateError(message)
      case WRITE_TIMEOUT =>
        val consistency = ByteStringUtils.readConsistency(it)
        val received = it.getInt
        val blockFor = it.getInt
        val writeType = ByteStringUtils.readString(it)
        WriteTimeoutError(message, consistency, received, blockFor, writeType)
      case READ_TIMEOUT =>
        val consistency = ByteStringUtils.readConsistency(it)
        val received = it.getInt
        val blockFor = it.getInt
        val dataPresent = it.getByte != 0
        ReadTimeoutError(message, consistency, received, blockFor, dataPresent)

      case SYNTAX_ERROR => SyntaxError(message)
      case UNAUTHORIZED => UnauthorizedError(message)
      case INVALID => InvalidError(message)
      case CONFIG_ERROR => ConfigError(message)
      case ALREADY_EXISTS =>
        val ksName = ByteStringUtils.readString(it)
        val cfName = ByteStringUtils.readString(it)
        AlreadyExistsError(message, ksName, cfName)
      case UNPREPARED =>
        val length = it.getShort
        val bytes = it.take(length).toArray
        it.drop(length)
        val digest = new MD5Digest(bytes)
        UnpreparedError(message, digest)
      case x =>
        throw new InternalException(s"Unknown error code $x")
    }
  }


  private[this] val VoidCode = 1
  private[this] val RowsCode = 2
  private[this] val SetKeyspaceCode = 3
  private[this] val PreparedCode = 4
  private[this] val SchemaChangeCode = 5

  private def decodeResult(bs: ByteString)(implicit byteOrder: ByteOrder): Result = {
    val it = bs.iterator

    val resultCode = it.getInt
    resultCode match {
      case VoidCode => Successful
      case RowsCode => ResultDecoder.decodeRows(it)
      case x => throw new OperationNotSupportedException(s"Decoder for response code $x not implemented.")
    }

  }
}

