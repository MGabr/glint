package glint.messages.server.request

/**
  * A pull average request
  *
  * @param rows The array of row indices to average
  */
private[glint] case class PullAverageRows(rows: Array[Array[Long]]) extends Request
