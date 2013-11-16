package com.vast.verona.client.pipeline

import akka.util.ByteString
import java.nio.ByteOrder
import com.vast.verona.data.{Consistency, CValue}
import com.vast.verona.client.{Startup, Request, Query}

private[pipeline] object RequestEncoder {

  def encode(r: Request)(implicit byteOrder: ByteOrder): (RequestOpcode, ByteString) = {
    r match {
      case Startup => EncodedStartup
      case q: Query => encodeQuery(q)
    }
  }

  private implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  //TODO: When compression is added, this is no longer a static value
  private[this] val EncodedStartup = (RequestOpcodes.STARTUP, ByteStringUtils.stringMap(Map("CQL_VERSION" -> "3.0.0")))

  private def encodeQuery(request: Query)(implicit byteOrder: ByteOrder): (RequestOpcode, ByteString) = {
    val suffixBuilder = ByteStringUtils.newBuilder(0)

    var flags: Int = 0

    if (!request.params.isEmpty) {
      flags = flags | 1
      suffixBuilder.putShort(request.params.length)
      request.params.foreach {
        param =>
          suffixBuilder.append(ByteStringUtils.bytes(CValue.toByteString(param)))
      }
    }

    if (request.skipMetadata) {
      flags = flags | 2
    }

    if (request.resultPageSize.isDefined) {
      flags = flags | 4
      suffixBuilder.putInt(request.resultPageSize.get)
    }

    if (request.pagingState.isDefined) {
      flags = flags | 8
      val ps = request.pagingState.get
      suffixBuilder.putInt(ps.length)
      suffixBuilder.append(ps)
    }

    if (request.serialConsistency != Consistency.SERIAL) {
      flags = flags | 16
      suffixBuilder.putShort(request.serialConsistency.id)
    }

    val body = ByteStringUtils.writeLongString(request.query) ++
      ByteStringUtils.newBuilder(3).putShort(request.consistency.id).putByte(flags.toByte).result() ++
      suffixBuilder.result()

    (RequestOpcodes.QUERY, body)
  }
}
