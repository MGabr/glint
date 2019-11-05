package glint.messages.server.request

/**
  * A pull norm dots request
  *
  * @param startRow The start index of the range of rows whose own partial dot products to get
  * @param endRow The exclusive end index of the range of rows whose own partial dot products to get
  */
private[glint] case class PullNormDots(startRow: Int, endRow: Int) extends Request
