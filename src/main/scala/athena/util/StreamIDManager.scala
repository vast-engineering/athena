package athena.util

import athena.Athena
import com.typesafe.scalalogging.slf4j.Logging

/**
 * A mutable, stateful manager for stream IDs. An instance of this class can manage the assignment and release
 * of 128 potential stream IDs.
 *
 * As mentioned above, this class is stateful, and thus explicitly *NOT* thread safe. Only use this in an enivronment
 * ensured to be scoped to a single thread (say, for example, the implementation of an Actor)
 */
class StreamIDManager extends Logging {

  import java.util.{BitSet => JBitSet}

  private[this] val bits = new JBitSet(128)

  def nextId(): Option[Byte] = {
    val index = bits.nextClearBit(0)
    if(index == -1) {
      logger.warn("Connection has no free stream IDs.")
      None
    } else {
      bits.set(index)
      Some(index.toByte)
    }
  }

  def release(index: Byte) {
    bits.clear(index)
  }

}
