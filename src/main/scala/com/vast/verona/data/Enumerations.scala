package com.vast.verona.data

object Consistency extends Enumeration {
  type Consistency = Value

  val ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE = Value

  def fromId(id: Int) = values.find(_.id == id)
}