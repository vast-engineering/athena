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
   * Parse a collection of bytes from the given ByteString, returning the remainder. It's assumed that the
   * start of the ByteString contains a short describing the length of the array. The given number of bytes
   * are parsed from teh string, and the remainder are returned.
   */
  def getShortBytes(bs: ByteString)(implicit byteOrder: ByteOrder): (ByteString, ByteString) = {
    val it = bs.iterator
    val size = it.getShort
    it.toByteString.splitAt(size)
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

    @tailrec def helper(count: Int, input: ByteString, acc: List[ByteString]): List[ByteString] = {
      if(count == 0) {
        acc.reverse
      } else {
        val (data, remainder) = getShortBytes(input)
        helper(count - 1, remainder, data :: acc)
      }
    }

    helper(count, bs, Nil)
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

    @tailrec def helper(count: Int, input: ByteString, acc: List[(ByteString, ByteString)]): List[(ByteString, ByteString)] = {
      if(count == 0) {
        acc.reverse
      } else {
        val (keyData, keyRemainder) = getShortBytes(input)
        val (valueData, valueRemainder) = getShortBytes(keyRemainder)
        helper(count - 1, valueRemainder, (keyData, valueData) :: acc)
      }
    }

    helper(count, it.toByteString, Nil)
  }




}
