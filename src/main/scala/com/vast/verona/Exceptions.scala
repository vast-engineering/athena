package com.vast.verona

class CassandraException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) { this(message, null) }
  def this(cause: Throwable) { this(null, cause) }
  def this() { this(null, null) }
}

/**
 * Thrown when the client experiences an unrecoverable error, or has reached an inconsistent state. This exception
 * normally signifies an internal implementation problem or bug.
 */
class InternalException(message: String, cause: Throwable) extends CassandraException(message, cause) {
  def this(message: String) { this(message, null) }
}

class ConnectionUnavailableException(message: String) extends CassandraException(message)



