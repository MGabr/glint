package glint.messages.server.request

/**
  * A pull request for matrices
  *
  * @param rows The row indices
  * @param cols The column indices
  */
private[glint] case class PullMatrix(rows: Array[Int], cols: Array[Int]) extends Request
