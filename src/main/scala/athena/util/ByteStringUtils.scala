package athena.util

import akka.util.{ByteIterator, ByteStringBuilder, ByteString}
import java.util.UUID
import java.nio.ByteOrder
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import athena.{Consistency, Athena}
import Consistency.Consistency
import athena.{Consistency, Athena}
import Consistency.Consistency
import java.net.{InetAddress, InetSocketAddress}

/**
 * Utilities to help encode and decode the grammar described in the spec
 *
 * From the spec:
 *   [int]          A 4 bytes integer
 *   [short]        A 2 bytes unsigned integer
 *   [string]       A [short] n, followed by n bytes representing an UTF-8
 *                  string.
 *   [long string]  An [int] n, followed by n bytes representing an UTF-8 string.
 *   [uuid]         A 16 bytes long uuid.
 *   [string list]  A [short] n, followed by n [string].
 *   [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
 *                  no byte should follow and the value represented is `null`.
 *   [short bytes]  A [short] n, followed by n bytes if n >= 0.
 *
 *   [option]       A pair of <id><value> where <id> is a [short] representing
 *                  the option id and <value> depends on that option (and can be
 *                  of size 0). The supported id (and the corresponding <value>)
 *                  will be described when this is used.
 *   [option list]  A [short] n, followed by n [option].
 *   [inet]         An address (ip and port) to a node. It consists of one
 *                  [byte] n, that represents the address size, followed by n
 *                  [byte] representing the IP address (in practice n can only be
 *                  either 4 (IPv4) or 16 (IPv6)), following by one [int]
 *                  representing the port.
 *   [consistency]  A consistency level specification. This is a [short]
 *                  representing a consistency level with the following
 *                  correspondance:
 *                    0x0000    ANY
 *                    0x0001    ONE
 *                    0x0002    TWO
 *                    0x0003    THREE
 *                    0x0004    QUORUM
 *                    0x0005    ALL
 *                    0x0006    LOCAL_QUORUM
 *                    0x0007    EACH_QUORUM
 *                    0x0008    SERIAL
 *                    0x0009    LOCAL_SERIAL
 *                    0x0010    LOCAL_ONE
 *
 *   [string map]      A [short] n, followed by n pair <k><v> where <k> and <v>
 *                     are [string].
 *   [string multimap] A [short] n, followed by n pair <k><v> where <k> is a
 *                     [string] and <v> is a [string list].
 *
 */
private[athena] object ByteStringUtils {

  def writeInt(i: Int)(implicit byteOrder: ByteOrder): ByteString = newBuilder(4).putInt(i).result()
  def readInt(bs: ByteString)(implicit byteOrder: ByteOrder): (Int, ByteString) = {
    val it = bs.iterator
    (it.getInt, it.toByteString)
  }
  
  def writeShort(i: Int)(implicit byteOrder: ByteOrder): ByteString = newBuilder(2).putShort(i).result()
  def readShort(bs: ByteString)(implicit byteOrder: ByteOrder): (Int, ByteString) = {
    val it = bs.iterator
    (it.getShort, it.toByteString)
  }
  
  def writeString(v: String)(implicit byteOrder: ByteOrder): ByteString = {
    //interesting note - using the version of getBytes() that takes a String rather than a Charset
    //is actually *faster* - String caches the encoder internally.
    val bytes = v.getBytes("UTF-8")
    newBuilder(bytes.length + 2).putShort(bytes.length).putBytes(bytes).result()
  }
  def readString(it: ByteIterator)(implicit byteOrder: ByteOrder): String = {
    val length = it.getShort
    val bytes = it.clone().take(length).toArray
    it.drop(length)
    new String(bytes, "UTF-8")
  }

  def writeLongString(v: String)(implicit byteOrder: ByteOrder): ByteString = {
    val bytes = v.getBytes("UTF-8")
    newBuilder(bytes.length + 4).putInt(bytes.length).putBytes(bytes).result()
  }
  def readLongString(it: ByteIterator)(implicit byteOrder: ByteOrder): String = {
    val length = it.getInt
    val bytes = it.clone().take(length).toArray
    it.drop(length)
    new String(bytes, "UTF-8")
  }

  def writeStringList(l: Seq[String])(implicit byteOrder: ByteOrder): ByteString = {
    val builder = ByteString.newBuilder
    var count = 0
    val it = l.iterator
    while(it.hasNext) {
      val bytes = it.next().getBytes("UTF-8")
      builder.putShort(bytes.length).putBytes(bytes)
      count = count + 1
    }
    newBuilder(2).putShort(count).append(builder.result()).result()
  }
  def readStringList(it: ByteIterator)(implicit byteOrder: ByteOrder): Seq[String] = {
    val builder = Seq.newBuilder[String]
    var numStrings: Int = it.getShort
    while(numStrings > 0) {
      builder += readString(it)
      numStrings = numStrings - 1
    }
    builder.result()
  }

  def readConsistency(it: ByteIterator)(implicit byteOrder: ByteOrder): Consistency = {
    val consistencyCode = it.getShort
    Consistency.fromId(consistencyCode).getOrElse(throw new Athena.InternalException(s"Unknown consistency value $consistencyCode"))
  }

  def uuid(v: UUID)(implicit byteOrder: ByteOrder): ByteString = {
    newBuilder(16).putLong(v.getMostSignificantBits).putLong(v.getLeastSignificantBits).result()
  }

  def bytes(v: ByteString)(implicit byteOrder: ByteOrder): ByteString = {
    newBuilder(4 + v.length).putInt(v.length).append(v).result()
  }
  def readBytes(it: ByteIterator)(implicit byteOrder: ByteOrder): ByteString = {
    val length = it.getInt
    val bs = it.clone().take(length).toByteString
    it.drop(length)
    bs
  }

  def shortBytes(v: Array[Byte])(implicit byteOrder: ByteOrder): ByteString = {
    val bb = ByteString.newBuilder
    bb.sizeHint(2 + v.length)
    bb.putShort(v.length).putBytes(v).result()
  }
  def shortBytes(v: ByteString)(implicit byteOrder: ByteOrder): ByteString = {
    newBuilder(2 + v.length).putShort(v.length).append(v).result()
  }
  def readShortBytes(it: ByteIterator)(implicit byteOrder: ByteOrder): ByteString = {
    val length = it.getShort
    val bs = it.clone().take(length).toByteString
    it.drop(length)
    bs
  }

  def stringMap(v: Map[String, String])(implicit byteOrder: ByteOrder): ByteString = {
    var count = 0
    val it = v.iterator
    val bb = newBuilder(2)
    while(it.hasNext) {
      val (key, value) = it.next()
      val keyBytes = key.getBytes("UTF-8")
      val valueBytes = value.getBytes("UTF-8")
      bb.putShort(keyBytes.length).putBytes(keyBytes).putShort(valueBytes.length).putBytes(valueBytes)
      count = count + 1
    }

    ByteString

    newBuilder(2).putShort(count).append(bb.result()).result()
  }

  def newBuilder(size: Int): ByteStringBuilder = {
    val bb = ByteString.newBuilder
    bb.sizeHint(size)
    bb
  }

  def readInetAddress(it: ByteIterator)(implicit byteOrder: ByteOrder): InetSocketAddress = {
    val size = it.getByte
    val addrBytes = new Array[Byte](size)
    it.getBytes(addrBytes)
    val port = it.getInt
    new InetSocketAddress(InetAddress.getByAddress(addrBytes), port)
  }

}
