package glint.models.client

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

/**
 * A big vector supporting basic parameter server element-wise operations
 *
 * {{{
 *   val vector: BigFMPairVector = ...
 *
 *   // item feature indices and weights for an exemplary batch size of 1
 *   val indices = Array(Array(0, 7, 10, 21, 23))
 *   val weights = Array(Array(1.0, 0.5, 0.5, 1.0, -1.0))
 *
 *   val (f, cacheKeys) = vector.pullSum(indices, weights)  // compute sums for gradient update
 *   val g = ...(f)  // compute whole BPR gradient
 *   vector.pushSum(g, cacheKeys)  // adjust vector by gradient updates
 *
 *   vector.destroy() // Destroy vector, freeing up memory on the parameter server
 * }}}
 */
trait BigFMPairVector extends BigVector[Float] {

  /**
   * Pull the weighted sums of the feature indices.
   *
   * @param indices The feature indices
   * @param weights The feature weights
   * @param ec The implicit execution context in which to execute the request
   * @return A future containing the weighted sums of the feature indices
   */
  def pullSum(indices: Array[Array[Int]], weights: Array[Array[Float]])
             (implicit ec: ExecutionContext): Future[Array[Float]]

  /**
   *
   * @param g
   * @param cacheKey
   * @param ec
   * @return
   */
  def pushSum(g: Array[Float], cacheKey: Int)(implicit ec: ExecutionContext): Future[Boolean]
}