package glint.messages.server.request

/**
  * A pull dot products request for FM-Pair
  *
  * @param iUser The user feature indices
  * @param wUser The user feature weights
  * @param iItem The item feature indices
  * @param wItem The item feature weights
  * @param cache Whether the indices, weights and sums should be cached
  */
private[glint] case class PullDotProdFM(iUser: Array[Array[Int]],
                                        wUser: Array[Array[Float]],
                                        iItem: Array[Array[Int]],
                                        wItem: Array[Array[Float]],
                                        cache: Boolean) extends Request
