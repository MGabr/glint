package glint.messages.server.request

/**
 * A pull sum request for FM-Pair
 *
 * @param indices The feature indices
 * @param weights The feature weights
 */
private[glint] case class PullSumFM(indices: Array[Array[Int]], weights: Array[Array[Float]]) extends Request