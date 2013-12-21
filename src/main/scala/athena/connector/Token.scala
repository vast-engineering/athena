package athena.connector

import akka.util.ByteString
import java.security.MessageDigest
import athena.M3PHash

sealed trait Token

case class M3PToken(value: Long) extends Token
case class RPToken(value: BigInt) extends Token
case class OPPToken(value: ByteString) extends Token

sealed trait TokenFactory {
  def fromString(string: String): Token
  def hash(partitionKey: ByteString): Token
}

object TokenFactory {
  def byName(partitionerName: String): Option[TokenFactory] = {
    if (partitionerName.endsWith("Murmur3Partitioner"))
      Some(M3PTokenFactory)
    else if (partitionerName.endsWith("RandomPartitioner"))
      Some(RPTokenFactory)
    else if (partitionerName.endsWith("OrderedPartitioner"))
      Some(OPPTokenFactory)
    else
      None
  }

  private object OPPTokenFactory extends TokenFactory {
    def fromString(string: String): Token = OPPToken(ByteString.fromString(string))
    def hash(partitionKey: ByteString): Token = OPPToken(partitionKey)
  }

  private object RPTokenFactory extends TokenFactory {
    def fromString(string: String): Token = RPToken(BigInt(string))

    def hash(partitionKey: ByteString): Token = {
      val digest = MessageDigest.getInstance("MD5")
      digest.digest(partitionKey.toArray)
      RPToken(BigInt(digest.digest()).abs)
    }
  }

  private object M3PTokenFactory extends TokenFactory {
    def fromString(string: String): Token = M3PToken(string.toLong)

    def hash(partitionKey: ByteString): Token = {
      val v = M3PHash.hash(partitionKey)
      M3PToken(if(v == Long.MinValue) Long.MaxValue else v)
    }
  }



}


