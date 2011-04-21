package revolute

class QueryException(msg: String, cause: Throwable) extends RuntimeException(msg, cause) {
  def this(msg: String) = this(msg, null)
  def this(cause: Throwable) = this(cause.getMessage, cause)
}