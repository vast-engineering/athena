package com.vast.verona.client

import akka.util.ByteString
import com.vast.verona.data.{Consistency, CValue}
import Consistency._
import com.vast.verona.data.CValue


sealed trait Request

//internal use only!
private[client] case object Startup extends Request


case class Query(query: String, consistency: Consistency = Consistency.QUORUM, params: IndexedSeq[CValue] = IndexedSeq(),
                 skipMetadata: Boolean = false, resultPageSize: Option[Int] = None,
                 pagingState: Option[ByteString] = None, serialConsistency: Consistency = Consistency.SERIAL) extends Request {
  require(serialConsistency == Consistency.SERIAL || serialConsistency == Consistency.LOCAL_SERIAL, "Serial consistency must be SERIAL or LOCAL_SERIAL.")
}


