package athena.connector.pipeline

import akka.io._
import athena._
import athena.connector.pipeline.messages._
import akka.util.ByteString
import athena.connector.pipeline.messages.ResponseOpcodes.{ERROR, RESULT, READY}
import athena.connector.CassandraRequests.CassandraRequest
import athena.connector.CassandraResponses.{Ready, CassandraResponse}
import athena.connector.HasConnectionInfo

case class RequestEnvelope(streamId: Byte, request: CassandraRequest)

/**
 * An event sent by the connection actor to signal a response for a request
 */
case class ResponseEnvelope(streamId: Byte, response: CassandraResponse)

class MessageStage extends PipelineStage[HasConnectionInfo, RequestEnvelope, Frame, ResponseEnvelope, Frame] {
  override def apply(ctx: HasConnectionInfo): PipePair[RequestEnvelope, Frame, ResponseEnvelope, Frame] = new PipePair[RequestEnvelope, Frame, ResponseEnvelope, Frame] {

    implicit val byteOrder = ctx.byteOrder

    val commandPipeline: (RequestEnvelope) => Iterable[Either[ResponseEnvelope, Frame]] = {
      case envelope: RequestEnvelope =>
        val (opcode, body) = RequestEncoder.encode(envelope.request)
        ctx.singleCommand(Frame(envelope.streamId, opcode.code, body))
    }

    val eventPipeline: (Frame) => Iterable[Either[ResponseEnvelope, Frame]] = { frame =>
      val opcode = ResponseOpcodes.findByCode(frame.opcode).getOrElse(throw new Athena.InternalException(s"Unknown opcode ${frame.opcode}"))

      ctx.singleEvent(ResponseEnvelope(frame.streamId, decodeResponse(opcode, frame.body)))
    }

    private def decodeResponse(opcode: ResponseOpcode, body: ByteString) = {
      opcode match {
        case READY => Ready
        case RESULT => ResponseDecoder.decodeResult(body)
        case ERROR => ResponseDecoder.decodeError(body, ctx.host)
        case x => throw new UnsupportedOperationException(s"Response parser for $opcode is not implemented.")
        //      case ERROR =>
        //      case AUTHENTICATE =>
        //      case SUPPORTED =>
        //      case EVENT =>
        //      case AUTH_CHALLENGE =>
        //      case AUTH_SUCCESS =>
      }

    }

  }
}