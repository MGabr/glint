package glint.messages.server.request

/**
  * A pull multiply request
  *
  * @param vector The partial vector with which to multiply the partial matrix
  */
private[glint] case class PullMultiply(vector: Array[Float]) extends Request
