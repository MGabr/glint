package glint.messages.server.request

/**
  * A pull average request
  *
  * @param rows The indices
  */
private[glint] case class PullAverageRow(rows: Array[Long]) extends Request
