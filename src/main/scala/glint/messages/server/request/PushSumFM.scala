package glint.messages.server.request

/**
 * A push sum request for FM-Pair
 *
 * @param id The push identification and cache key to retrieve the cached indices and weights
 * @param g The partial gradients per training instance in the batch
 */
private[glint] case class PushSumFM(id: Integer, g: Array[Float]) extends Request
