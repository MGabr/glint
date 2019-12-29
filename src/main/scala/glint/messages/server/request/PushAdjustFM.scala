package glint.messages.server.request

/**
  * A push adjust request for FM-Pair
  *
  * @param g The general BPR gradient per training instance in the batch
  * @param cacheKey The key to retrieve the cached indices and weights
  */
private[glint] case class PushAdjustFM(g: Array[Float], cacheKey: Int) extends Request
