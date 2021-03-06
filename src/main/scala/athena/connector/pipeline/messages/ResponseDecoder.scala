package athena.connector.pipeline.messages

import athena._
import akka.util.ByteString
import java.nio.ByteOrder
import athena.util.{MD5Hash, ByteStringUtils}
import java.net.InetSocketAddress

import athena.connector.CassandraResponses._
import Errors._
import athena.connector.CassandraError
import athena.Athena.InternalException

private[pipeline] object ResponseDecoder {

  def decodeError(bs: ByteString, host: InetSocketAddress)(implicit byteOrder: ByteOrder): CassandraError = {
    val it = bs.iterator
    val errorCode = it.getInt
    val message = ByteStringUtils.readString(it)

    import ErrorCodes._

    errorCode match {
      case SERVER_ERROR => ServerError(message, host)
      case PROTOCOL_ERROR => ProtocolError(message)
      case BAD_CREDENTIALS => CredentialsError(message, host)

      case UNAVAILABLE =>
        val consistency = ByteStringUtils.readConsistency(it)
        val required = it.getInt
        val alive = it.getInt
        UnavailableError(message, consistency, required, alive)
      case OVERLOADED => OverloadedError(message, host)
      case IS_BOOTSTRAPPING => IsBootstrappingError(message, host)
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
        val bytes = it.take(length).toByteString
        it.drop(length)
        UnpreparedError(message, host, MD5Hash(bytes))
      case x =>
        throw new Athena.InternalException(s"Unknown error code $x")
    }
  }


  private[this] val VoidCode = 1
  private[this] val RowsCode = 2
  private[this] val SetKeyspaceCode = 3
  private[this] val PreparedCode = 4
  private[this] val SchemaChangeCode = 5

  def decodeResult(bs: ByteString)(implicit byteOrder: ByteOrder): Result = {

    val it = bs.iterator

    val resultCode = it.getInt
    resultCode match {
      case VoidCode => SuccessfulResult
      case RowsCode => ResultDecoder.decodeRows(it)
      case SetKeyspaceCode => ResultDecoder.decodeSetKeyspace(it)
      case PreparedCode => ResultDecoder.decodePepared(it)
      case SchemaChangeCode => ResultDecoder.decodeSchemaChange(it)
      case x => throw new NotImplementedError(s"Decoder for response code $x not implemented.")
    }
  }

  def decodeEvent(bs: ByteString)(implicit byteOrder: ByteOrder): ClusterEvent = {
    val it = bs.iterator
    val eventName = ByteStringUtils.readString(it)
    val subEventName = ByteStringUtils.readString(it)
    eventName match {
      case "TOPOLOGY_CHANGE" =>
        val nodeAddr = ByteStringUtils.readInetAddress(it)
        TopologyEvent(subEventName, nodeAddr)
      case "STATUS_CHANGE" =>
        val nodeAddr = ByteStringUtils.readInetAddress(it)
        HostStatusEvent(subEventName, nodeAddr)
      case "SCHEMA_CHANGE" =>
        val keyspace = ByteStringUtils.readString(it)
        val table = ByteStringUtils.readString(it)
        SchemaEvent(subEventName, keyspace, table)
      case x =>
        throw new InternalException(s"Unknown event $x")
    }
  }

}

