package athena.connector.pipeline.messages

import scala.collection.mutable

sealed abstract class RequestOpcode(val code: Byte)

object RequestOpcodes  {
  case object STARTUP extends RequestOpcode(1)
  case object OPTIONS extends RequestOpcode(5)
  case object QUERY extends RequestOpcode(7)
  case object PREPARE extends RequestOpcode(9)
  case object EXECUTE extends RequestOpcode(10)
  case object REGISTER extends RequestOpcode(11)
  case object BATCH extends RequestOpcode(13)
  case object AUTH_RESPONSE extends RequestOpcode(15)
}


sealed abstract class ResponseOpcode(val code: Byte)
object ResponseOpcodes {

  //scala enumerations are dumb. Should probably just punt and implement this in a single Java file.
  case object ERROR extends ResponseOpcode(0)
  case object READY extends ResponseOpcode(2)
  case object AUTHENTICATE extends ResponseOpcode(3)
  case object SUPPORTED extends ResponseOpcode(5)
  case object RESULT extends ResponseOpcode(8)
  case object EVENT extends ResponseOpcode(12)
  case object AUTH_CHALLENGE extends ResponseOpcode(14)
  case object AUTH_SUCCESS extends ResponseOpcode(16)

  private[this] val values = Map(
    0 -> ERROR,
    2 -> READY,
    3 -> AUTHENTICATE,
    5 -> SUPPORTED,
    8 -> RESULT,
    12 -> EVENT,
    13 -> AUTH_CHALLENGE,
    16 -> AUTH_SUCCESS
  )

  def findByCode(code: Byte): Option[ResponseOpcode] = values.get(code)

}

object ErrorCodes {
  val SERVER_ERROR = 0x0000
  val PROTOCOL_ERROR = 0x000A
  val BAD_CREDENTIALS = 0x0100

  // 1xx: problem during request execution
  val UNAVAILABLE = 0x1000
  val OVERLOADED =  0x1001
  val IS_BOOTSTRAPPING = 0x1002
  val TRUNCATE_ERROR = 0x1003
  val WRITE_TIMEOUT = 0x1100
  val READ_TIMEOUT = 0x1200

  // 2xx: problem validating the request
  val SYNTAX_ERROR = 0x2000
  val UNAUTHORIZED  = 0x2100
  val INVALID = 0x2200
  val CONFIG_ERROR = 0x2300
  val ALREADY_EXISTS = 0x2400
  val UNPREPARED = 0x2500
}


