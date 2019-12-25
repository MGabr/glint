package glint.messages.server.request

/**
  * A push adjust request for FM-Pair
  *
  * @param id The push identification and cache key to retrieve the cached indices and weights
  * @param g The general BPR gradient per training instance in the batch
  */
private[glint] case class PushAdjustFM(id: Integer, g: Array[Float]) extends Request
