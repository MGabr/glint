package glint.messages.server.response

import glint.messages.server.request.Request

/**
 * A response containing sums
 *
 * @param s sums
 * @param cacheKey key to retrieve the cached indices and weights
 */
private[glint] case class ResponsePullSumFM(s: Array[Float], cacheKey: Int) extends Request