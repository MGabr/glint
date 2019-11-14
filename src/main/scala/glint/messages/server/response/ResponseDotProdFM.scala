package glint.messages.server.response

import glint.messages.server.request.Request

/**
 * A response containing dot products
 *
 * @param f dot products
 * @param cacheKey key to retrieve the cached indices and weights
 */
private[glint] case class ResponseDotProdFM(f: Array[Float], cacheKey: Int) extends Request