package glint.models.client

import org.apache.hadoop.conf.Configuration

import scala.concurrent.{ExecutionContext, Future}

/**
  * A big matrix of floats and supporting specific parameter server operations
  * for efficient distributed pairwise factorization machine training
  *
  * {{{
  *   val matrix: BigFMPairMatrix = ...
  *
  *   // user and item feature indices and weights for an exemplary batch size of 1
  *   val iUser = Array(Array(0, 7))
  *   val wUser = Array(Array(1.0, 0.33))
  *   val iItem = Array(Array(10, 15, 20))
  *   val wItem = Array(Array(1.0, 0.25, 0.25))
  *
  *   // active feature aggregation method
  *   val (s, cacheKeys) = matrix.pullSum(iUser ++ iItem, wUser ++ wItem)
  *   val g = ...(s) // compute cross-batch BPR gradient
  *   matrix.pushSum(g, cacheKeys)
  *
  *   // dimension aggregation method
  *   val (f, cacheKeys) = matrix.dotprod(iUser, wUser, iItem, wItem)
  *   val g = ...(f) // compute BPR utility function gradient
  *   matrix.adjust(g, cacheKeys)
  *
  *   matrix.destroy() // Destroy matrix, freeing up memory on the parameter server
  * }}}
  */
trait BigFMPairMatrix extends BigMatrix[Float] {

  /**
    * The number of partitions
    */
  private[glint] val numPartitions: Int

  /**
    * Saves the matrix to HDFS
    *
    * @param hdfsPath The HDFS base path where the matrix should be saved
    * @param hadoopConfig The Hadoop configuration to use for saving the data to HDFS
    * @param trainable Whether the saved matrix should be retrainable, requiring more data being saved
    * @param ec The implicit execution context in which to execute the request
    * @return A future whether the matrix was successfully saved
    */
  def save(hdfsPath: String, hadoopConfig: Configuration, trainable: Boolean)
          (implicit ec: ExecutionContext): Future[Boolean]

  /**
    * Computes the dot products
    *
    * @param iUser The user feature indices
    * @param wUser The user feature weights
    * @param iItem The item feature indices
    * @param wItem The item feature weights
    * @param cache Whether the indices, weights and sums should be cached. Not required for recommendation
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the dot products and the cache keys for the adjust operation
    */
  def dotprod(iUser: Array[Array[Int]],
              wUser: Array[Array[Float]],
              iItem: Array[Array[Int]],
              wItem: Array[Array[Float]],
              cache: Boolean = true)(implicit ec: ExecutionContext): Future[(Array[Float], Array[Int])]

  /**
   * Adjusts the weights according to the received gradient updates
   *
   * @param g The general BPR gradient per training instance in the batch
   * @param cacheKeys The keys to retrieve the cached indices and weights
   * @param ec The implicit execution context in which to execute the request
   * @return A future containing either the success or failure of the operation
   */
  def adjust(g: Array[Float], cacheKeys: Array[Int])(implicit ec: ExecutionContext): Future[Boolean]

  /**
   * Pull the weighted sums of the feature indices
   *
   * @param keys The feature indices
   * @param weights The feature weights
   * @param cache Whether the indices and weights should be cached. Not required for recommendation
   * @param ec The implicit execution context in which to execute the request
   * @return A future containing the weighted sums of the feature indices
   */
  def pullSum(keys: Array[Array[Int]], weights: Array[Array[Float]], cache: Boolean = true)
             (implicit ec: ExecutionContext): Future[(Array[Array[Float]], Array[Int])]


  /**
   * Adjusts the weights according to the received sum gradient updates
   *
   * @param g The BPR gradients per training instance in the batch
   * @param cacheKeys The keys to retrieve the cached indices and weights
   * @param ec The implicit execution context in which to execute the request
   * @return A future containing either the success or failure of the operation
   */
  def pushSum(g: Array[Array[Float]], cacheKeys: Array[Int])(implicit ec: ExecutionContext): Future[Boolean]
}
