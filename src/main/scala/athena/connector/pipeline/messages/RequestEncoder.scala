package athena.connector.pipeline.messages

import akka.util.ByteString
import java.nio.ByteOrder

import athena.connector._
import athena.data._
import athena.connector.CassandraRequests._
import athena.util.ByteStringUtils
import athena.Consistency

private[connector] object RequestEncoder {

  def encode(r: CassandraRequest)(implicit byteOrder: ByteOrder): (RequestOpcode, ByteString) = {
    r match {
      case Startup =>
        (RequestOpcodes.STARTUP, EncodedStartup)
      case QueryRequest(query, params) =>
        (RequestOpcodes.QUERY, ByteStringUtils.writeLongString(query) ++ encodeQueryParams(params))
      case ExecuteRequest(statementId, params, excludeMetadata) =>
        (RequestOpcodes.EXECUTE, ByteStringUtils.shortBytes(statementId.toByteString) ++ encodeQueryParams(params, excludeMetadata))
      case Register(eventNames) =>
        (RequestOpcodes.REGISTER, ByteStringUtils.writeStringList(eventNames.map(_.toString)))
      case Prepare(query) =>
        (RequestOpcodes.PREPARE, ByteStringUtils.writeLongString(query))
    }
  }

  private implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  //TODO: When compression is added, this is no longer a static value
  private[this] val EncodedStartup = ByteStringUtils.stringMap(Map("CQL_VERSION" -> "3.0.0"))

  private def encodeQueryParams(queryParams: QueryParams, excludeMetadata: Boolean = false) = {
    val suffixBuilder = ByteStringUtils.newBuilder(0)

    var flags: Int = 0

    if (!queryParams.params.isEmpty) {
      flags = flags | 1
      suffixBuilder.putShort(queryParams.params.length)
      queryParams.params.foreach { param =>
        suffixBuilder.append(ByteStringUtils.bytes(param.as[ByteString]))
      }
    }

    if(excludeMetadata) {
      flags = flags | 2
    }

    if (queryParams.resultPageSize.isDefined) {
      flags = flags | 4
      suffixBuilder.putInt(queryParams.resultPageSize.get)
    }

    if (queryParams.pagingState.isDefined) {
      flags = flags | 8
      val ps = queryParams.pagingState.get
      suffixBuilder.putInt(ps.length)
      suffixBuilder.append(ps)
    }

    if (queryParams.serialConsistency != Consistency.SERIAL) {
      flags = flags | 16
      suffixBuilder.putShort(queryParams.serialConsistency.id)
    }

    ByteStringUtils.newBuilder(3).putShort(queryParams.consistency.id).putByte(flags.toByte).result() ++
      suffixBuilder.result()
  }


}
