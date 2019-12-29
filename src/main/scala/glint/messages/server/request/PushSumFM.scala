package glint.messages.server.request

/**
 * A push sum request for FM-Pair
 *
 * @param g The partial gradients per training instance in the batch
 * @param cacheKey The key to retrieve the cached indices and weights
 */
private[glint] case class PushSumFM(g: Array[Float], cacheKey: Int) extends Request
