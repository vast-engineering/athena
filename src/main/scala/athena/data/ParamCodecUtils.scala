package athena.data

import akka.util.{ByteIterator, ByteString}
import java.nio.ByteOrder
import scala.annotation.tailrec
import com.typesafe.scalalogging.slf4j.Logging
import athena.util.ByteStringUtils

/**
 * Misc utils used to help encode parameter values.
 *
 */
private[data] object ParamCodecUtils extends Logging {

  import ByteStringUtils.newBuilder

  /**
   * Parse a length prefixed UTF-8 String from the beginning of this ByteString, returning the String
   * and the remainder of the ByteString
   */
  def readString(it: ByteIterator)(implicit byteOrder: ByteOrder): String = {
    val size = it.getShort
    new String(it.take(size).toArray, "UTF-8")
  }

  /**
   * Packs a Iterable of ByteString instances into the following structure -
   *
   *   [numElements] [size_n][bytes_n] where numElements and size are shorts, and the (size, byte) pair is repeated numElements times
   *
   */
  def packParamByteStrings(strings: Iterable[ByteString], numElements: Int)(implicit byteOrder: ByteOrder): ByteString = {
    val builder = newBuilder(2).putShort(numElements)
    val it = strings.iterator
    while(it.hasNext) {
      val string = it.next()
      builder.putShort(string.length)
      builder.append(string)
    }
    builder.result()
  }


  /**
   * Decodes a ByteString containing the following structure into a Seq[ByteString].
   *
   * [count][size_n][bytes_n]
   *
   * where count and size_n is are unsigned short values, and the (size, bytes) pair is repeated count times.
   */
  def unpackListParam(bs: ByteString)(implicit byteOrder: ByteOrder): Seq[ByteString] = {
    val it = bs.iterator
    val count = it.getShort

    val builder = Seq.newBuilder[ByteString]
    var idx = 0
    while(idx < count) {
      val size = it.getShort
      builder += it.clone().take(size).toByteString
      it.drop(size)
      idx = idx + 1
    }
    builder.result()
  }

  /**
   * Decodes a ByteString containing the following structure into a Seq[(ByteString, ByteString)].
   *
   * [count][size_key_n][bytes_key_n][size_value_n][bytes_value_n]
   *
   * where count and size is are unsigned short values, and the ([key bytes], [value bytes]) sequence is repeated count times.
   */
  def unpackMapParam(bs: ByteString)(implicit byteOrder: ByteOrder): Seq[(ByteString, ByteString)] = {
    val it = bs.iterator
    val count = it.getShort

    val builder = Seq.newBuilder[(ByteString, ByteString)]
    var idx = 0
    while(idx < count) {
      val keySize = it.getShort
      val keyBytes = it.clone().take(keySize).toByteString
      it.drop(keySize)
      val dataSize = it.getShort
      val dataBytes = it.clone().take(dataSize).toByteString
      it.drop(dataSize)
      builder += (keyBytes -> dataBytes)
      idx = idx + 1
    }
    builder.result()
  }




}
