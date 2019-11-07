package glint.messages.server.request

/**
  * A push adjust request for FM-Pair
  *
  * @param id       The push identification
  * @param g        The general BPR gradient per training instance in the batch
  * @param cacheKey The key to retrieve the cached indices and weights
  */
private[glint] case class PushAdjustFM(id: Integer, g: Array[Float], cacheKey: Int) extends Request
