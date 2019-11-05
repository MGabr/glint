package glint.messages.server.request

/**
  * A pull multiply request
  *
  * @param vector The partial vector with which to multiply the partial matrix
  * @param startRow The start row index of the matrix, to support multiplication with only a part of the partial matrix
  * @param endRow The exclusive end row index of the matrix
  */
private[glint] case class PullMultiply(vector: Array[Float], startRow: Int, endRow: Int) extends Request
