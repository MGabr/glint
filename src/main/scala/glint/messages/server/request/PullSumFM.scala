package glint.messages.server.request

/**
 * A pull sum request for FM-Pair
 *
 * @param indices The feature indices
 * @param weights The feature weights
 * @param cache Whether the indices and weights should be cached
 */
private[glint] case class PullSumFM(indices: Array[Array[Int]],
                                    weights: Array[Array[Float]],
                                    cache: Boolean) extends Request