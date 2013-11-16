package com.vast.verona.client.pipeline

import akka.io._
import com.vast.verona._
import java.nio.ByteOrder
import com.vast.verona.client.{Response, Request}

case class ResponseEnvelope(streamId: Byte, response: Response)
case class RequestEnvelope(streamId: Byte, request: Request)

class MessageStage extends PipelineStage[PipelineContext, RequestEnvelope, Frame, ResponseEnvelope, Frame] {
  override def apply(ctx: PipelineContext): PipePair[RequestEnvelope, Frame, ResponseEnvelope, Frame] = new PipePair[RequestEnvelope, Frame, ResponseEnvelope, Frame] {

    implicit val byteOrder = ByteOrder.BIG_ENDIAN

    val commandPipeline: (RequestEnvelope) => Iterable[Either[ResponseEnvelope, Frame]] = {
      case envelope: RequestEnvelope =>
        val (opcode, body) = RequestEncoder.encode(envelope.request)
        ctx.singleCommand(Frame(envelope.streamId, opcode.code, body))
    }

    val eventPipeline: (Frame) => Iterable[Either[ResponseEnvelope, Frame]] = { frame =>
      val opcode = ResponseOpcodes.findByCode(frame.opcode).getOrElse(throw new InternalException(s"Unknown opcode ${frame.opcode}"))
      val envelope = ResponseEnvelope(frame.streamId, ResponseDecoder.decode(opcode, frame.body))
      ctx.singleEvent(envelope)
    }

  }
}