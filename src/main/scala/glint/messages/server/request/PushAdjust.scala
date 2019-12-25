package glint.messages.server.request

/**
  * A push adjust request
  *
  * @param id The push identification and cache key to retrieve the cached indices and weights
  * @param gPlus The gradient updates for the input and output word combinations
  * @param gMinus The gradient updates for the input and random neighbour word combinations
  */
private[glint] case class PushAdjust(id: Integer, gPlus: Array[Float], gMinus: Array[Float]) extends Request
