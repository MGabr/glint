package glint.messages.server.request

/**
  * A push adjust request
  *
  * @param gPlus The gradient updates for the input and output word combinations
  * @param gMinus The gradient updates for the input and random neighbour word combinations
  * @param cacheKey The key to retrieve the cached indices and weights
  */
private[glint] case class PushAdjust(gPlus: Array[Float], gMinus: Array[Float], cacheKey: Int) extends Request
