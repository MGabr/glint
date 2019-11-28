package glint.models.client

import org.apache.hadoop.conf.Configuration

import scala.concurrent.{ExecutionContext, Future}

/**
 * A big vector of floats and supporting specific parameter server operations
 * for efficient distributed pairwise factorization machine training
 *
 * {{{
 *   val vector: BigFMPairVector = ...
 *
 *   // item feature indices and weights for an exemplary batch size of 1
 *   val indices = Array(Array(0, 7, 10, 21, 23))
 *   val weights = Array(Array(1.0, 0.5, 0.5, 1.0, -1.0))
 *
 *   val (f, cacheKeys) = vector.pullSum(indices, weights)
 *   val g = ...(f)  // compute BPR utility function gradient
 *   vector.pushSum(g, cacheKeys)
 *
 *   vector.destroy() // Destroy vector, freeing up memory on the parameter server
 * }}}
 */
trait BigFMPairVector extends BigVector[Float] {

  /**
   * The number of partitions
   */
  private[glint] val numPartitions: Int

  /**
   * Saves the vector to HDFS
   *
   * @param hdfsPath The HDFS base path where the vector should be saved
   * @param hadoopConfig The Hadoop configuration to use for saving the data to HDFS
   * @param trainable Whether the saved vector should be retrainable, requiring more data being saved
   * @param ec The implicit execution context in which to execute the request
   * @return A future whether the vector was successfully saved
   */
  def save(hdfsPath: String, hadoopConfig: Configuration, trainable: Boolean)
          (implicit ec: ExecutionContext): Future[Boolean]

  /**
   * Pull the weighted sums of the feature indices
   *
   * @param keys The feature indices
   * @param weights The feature weights
   * @param cache Whether the indices and weights should be cached. Not required for recommendation
   * @param ec The implicit execution context in which to execute the request
   * @return A future containing the weighted sums of the feature indices and the cache keys for the adjust operation
   */
  def pullSum(keys: Array[Array[Int]], weights: Array[Array[Float]], cache: Boolean = true)
             (implicit ec: ExecutionContext): Future[(Array[Float], Array[Int])]

  /**
   * Adjust the weights according to the received gradient updates
   *
   * @param g The general BPR gradient per training instance in the batch
   * @param cacheKeys The keys to retrieve the cached indices and weights
   * @param ec The implicit execution context in which to execute the request
   * @return A future containing either the success or failure of the operation
   */
  def pushSum(g: Array[Float], cacheKeys: Array[Int])(implicit ec: ExecutionContext): Future[Boolean]
}