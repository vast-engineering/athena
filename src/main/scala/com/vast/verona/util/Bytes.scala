package com.vast.verona.util

import java.lang.reflect.Constructor
import akka.util.ByteString
import scala.util.control.NonFatal
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Simple utility methods to make working with bytes (blob) easier.
 */
object Bytes extends Logging {

  /**
   * Converts a blob to its CQL hex string representation.
   * <p>
   * A CQL blob string representation consist of the hexadecimal
   * representation of the blob bytes prefixed by "0x".
   *
   * @param bytes the blob/bytes to convert to a string.
   * @return the CQL string representation of { @code bytes}. If { @code bytes}
   *         is { @code null}, this method returns { @code null}.
   */
  def toHexString(bytes: ByteString): String = {
    if (bytes == null) return null
    if (bytes.length == 0) return "0x"
    val array: Array[Char] = new Array[Char](2 * (bytes.length + 1))
    array(0) = '0'
    array(1) = 'x'
    toRawHexString(bytes, array, 2)
  }

  /**
   * Converts a blob to its CQL hex string representation.
   * <p>
   * A CQL blob string representation consist of the hexadecimal
   * representation of the blob bytes prefixed by "0x".
   *
   * @param byteArray the blob/bytes array to convert to a string.
   * @return the CQL string representation of { @code bytes}. If { @code bytes}
   *         is { @code null}, this method returns { @code null}.
   */
  def toHexString(byteArray: Array[Byte]): String = {
    toHexString(ByteString(byteArray))
  }

  /**
   * Parse an hex string representing a CQL blob.
   * <p>
   * The input should be a valid representation of a CQL blob, i.e. it
   * must start by "0x" followed by the hexadecimal representation of the
   * blob bytes.
   *
   * @param str the CQL blob string representation to parse.
   * @return the bytes corresponding to { @code str}. If { @code str}
   *         is { @code null}, this method returns { @code null}.
   *
   * @throws IllegalArgumentException if { @code str} is not a valid CQL
   *                                  blob string.
   */
  def fromHexString(str: String): ByteString = {
    if (str.length % 2 == 1) throw new IllegalArgumentException("A CQL blob string must have an even length (since one byte is always 2 hexadecimal character)")
    if (str.charAt(0) != '0' || str.charAt(1) != 'x') throw new IllegalArgumentException("A CQL blob string must start with \"0x\"")
    ByteString(fromRawHexString(str, 2))
  }


  private def toRawHexString(bytes: ByteString, array: Array[Char], offset: Int): String = {
    val size: Int = bytes.length
    assert(array.length >= offset + 2 * size)

    var index = 0
    val it = bytes.iterator
    while (it.hasNext) {
      val byteAsInt = it.next().toInt
      array(offset + (index * 2)) = byteToChar((byteAsInt & 0xf0) >> 4)
      array(offset + ((index * 2) + 1)) = byteToChar(byteAsInt & 0x0f)
      index = index + 1
    }

    wrapCharArray(array)
  }

  private def fromRawHexString(str: String, strOffset: Int): Array[Byte] = {
    val bufferLength = (str.length - strOffset) / 2
    val bytes: Array[Byte] = new Array[Byte](bufferLength)

    var i = 0
    while (i < bufferLength) {
      val halfByte1: Byte = charToByte(str.charAt(strOffset + i * 2))
      val halfByte2: Byte = charToByte(str.charAt(strOffset + i * 2 + 1))
      if (halfByte1 == -1 || halfByte2 == -1) {
        throw new IllegalArgumentException("Non-hex characters in " + str)
      }
      bytes(i) = ((halfByte1 << 4) | halfByte2).toByte
      i = i + 1
    }

    bytes
  }

  private def wrapCharArray(c: Array[Char]): String = {
    stringConstructor.flatMap { ctor =>
      try {
        Some(ctor.newInstance(0: java.lang.Integer, c.length: java.lang.Integer, c))
      } catch {
        case NonFatal(e) =>
          logger.debug("Couldn't create fast string instance - {}", e)
          None
      }
    } getOrElse {
      //use the slow constructor
      new String(c)
    }
  }

  private final val charToByte: Array[Byte] = {
    val bytes = new Array[Byte](256)
    for(c <- 0 to bytes.length) {
      if (c >= '0' && c <= '9')
        bytes(c) = (c - '0').toByte
      else if (c >= 'A' && c <= 'F')
        bytes(c) = (c - 'A' + 10).toByte
      else if (c >= 'a' && c <= 'f')
        bytes(c) = (c - 'a' + 10).toByte
      else
        bytes(c) = -1: Byte
    }
    bytes
  }
  private final val byteToChar: Array[Char] = {
    val chars = new Array[Char](16)
    for(i <- 0 to chars.length) {
      chars(i) = Integer.toHexString(i).charAt(0)
    }
    chars
  }

  /*
 * We use reflection to get access to a String protected constructor
 * (if available) so we can build avoid copy when creating hex strings.
 * That's stolen from Cassandra's code.
 */
  private val stringConstructor: Option[Constructor[String]] = {
    val ctor = try {
      Option(classOf[String].getDeclaredConstructor(classOf[Int], classOf[Int], classOf[Array[Char]]))
    } catch {
      case e: NoSuchMethodException =>
        None
      case e: SecurityException =>
        None
    }
    ctor.foreach(_.setAccessible(true))
    ctor
  }
}


