package com.vast.verona.client.pipeline

import akka.io.{SymmetricPipePair, PipePair, PipelineContext, SymmetricPipelineStage}

import akka.util.ByteString
import java.nio.ByteOrder

import scala.annotation.tailrec

case class Frame(flags: Byte, streamId: Byte, opcode: Byte, body: ByteString)

object Frame {
  def apply(streamId: Byte, opcode: Byte, body: ByteString): Frame = {
    Frame(0, streamId, opcode, body)
  }
}

object FrameStage {
  //Cassandra responses have an 8 byte header
  val HeaderSize = 8

  val CurrentProtocolVersion = 2
}

/**
 * This class is responsible for transforming a sequence of bytes from the network socket into a
 * series Frame objects.
 *
 * A cassandra frame is defined with the following structure -
 *
 *    0         8        16        24        32
 *    +---------+---------+---------+---------+
 *    | version |  flags  | stream  | opcode  |
 *    +---------+---------+---------+---------+
 *    |                length                 |
 *    +---------+---------+---------+---------+
 *    |                                       |
 *    .            ...  body ...              .
 *    .                                       .
 *    .                                       .
 *    +----------------------------------------
 *
 * @author David Pratt (dpratt@vast.com)
 */
class FrameStage extends SymmetricPipelineStage[PipelineContext, Frame, ByteString] {

  override def apply(ctx: PipelineContext): PipePair[Frame, ByteString, Frame, ByteString] = new SymmetricPipePair[Frame, ByteString] {

    import FrameStage._

    //used to hold the beginning of any incomplete frames
    var buffer = None: Option[ByteString]

    implicit val byteOrder = ByteOrder.BIG_ENDIAN

    /**
     * Extract as many complete frames as possible from the given ByteString
     * and return the remainder together with the extracted frames in reverse
     * order.
     */
    @tailrec
    def extractFrames(bs: ByteString, acc: List[Frame]): (Option[ByteString], Seq[Frame]) = {
      if (bs.isEmpty) {
        (None, acc)
      } else if (bs.length < HeaderSize) {
        (Some(bs.compact), acc)
      } else {
        val it = bs.iterator
        val version = it.getByte

        val maskedVersion = version & 0x7F
        if(maskedVersion != CurrentProtocolVersion) {
          throw new IllegalArgumentException(s"Unknown incoming frame version $maskedVersion, expected $CurrentProtocolVersion")
        }

        val flags = it.getByte
        val streamId = it.getByte
        val opcodeValue = it.getByte
        val bodyLength = it.getInt

        val fullFrameLength = bodyLength + HeaderSize

        if (bs.length >= fullFrameLength) {
          val remaining = bs.drop(fullFrameLength)
          val bodyBytes = bs.slice(HeaderSize, fullFrameLength)

          val frame = Frame(flags, streamId, opcodeValue, bodyBytes.compact)
          extractFrames(remaining, frame :: acc)
        } else {
          (Some(bs.compact), acc)
        }
      }
    }


    //transforms Frames into ByteStrings
    val commandPipeline: (Frame) => Iterable[Either[Frame, ByteString]] = { frame =>
      val bb = ByteString.newBuilder
      bb.putByte(CurrentProtocolVersion.toByte)
      bb.putByte(frame.flags)
      bb.putByte(frame.streamId)
      bb.putByte(frame.opcode)
      bb.putInt(frame.body.length)
      bb.append(frame.body)
      ctx.singleCommand(bb.result())
    }

    //transforms ByteStrings into frames
    val eventPipeline: (ByteString) => Iterable[Either[Frame, ByteString]] = { bs =>
      val data = if (buffer.isEmpty) bs else buffer.get ++ bs
      val (remainingBytes, frames) = extractFrames(data, Nil)
      buffer = remainingBytes

      frames match {
        case Nil =>
          //nothing to emit
          Nil
        case one :: Nil =>
          //optimized emit of a single event
          ctx.singleEvent(one)
        case many =>
          //frames come out of extractFrames in reverse order
          many.reverseMap(Left(_))
      }
    }
  }
}
