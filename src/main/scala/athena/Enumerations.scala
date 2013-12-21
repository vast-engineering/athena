package athena

object Consistency extends Enumeration {
  type Consistency = Value

  val ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE = Value

  def fromId(id: Int) = values.find(_.id == id)
}

object SerialConsistency extends Enumeration {
  type SerialConsistency = Value

  val SERIAL = Value(8)
  val LOCAL_SERIAL = Value(9)

  def fromId(id: Int) = values.find(_.id == id)
}
