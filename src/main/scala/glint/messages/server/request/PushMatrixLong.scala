package glint.messages.server.request

/**
  * A push request for matrices containing longs
  *
  * @param rows The row indices
  * @param cols The column indices
  * @param values The values to add
  */
private[glint] case class PushMatrixLong(id: Int, rows: Array[Long], cols: Array[Long], values: Array[Long]) extends Request
