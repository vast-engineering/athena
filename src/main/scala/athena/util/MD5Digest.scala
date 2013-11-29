package athena.util

import java.util

/**
 * The result of the computation of an MD5 digest.
 *
 * A MD5 is really just a byte[] but arrays are a no go as map keys. We could
 * wrap it in a ByteString but:
 *   1. MD5Digest is a more explicit name than ByteString to represent a md5.
 *   2. Using our own class allows to use our FastByteComparison for equals.
 *
 * This class was borrowed from the DataStax driver implementation.
 */
class MD5Digest(bytes: Array[Byte]) {

  override def hashCode(): Int = util.Arrays.hashCode(bytes)

  override def equals(obj: scala.Any): Boolean = super.equals(obj)

  override def toString: String = Bytes.toHexString(bytes)
}
