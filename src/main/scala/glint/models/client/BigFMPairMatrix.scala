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
  *   val (f, cacheKeys) = matrix.dotprod(iUser, wUser, iItem, wItem) // compute dot products for gradient updates
  *   val g = ...(f) // compute whole BPR gradient
  *   matrix.adjust(g, cacheKeys) // adjust matrix by gradient updates
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
    * @param hdfsPath     The HDFS base path where the matrix should be saved
    * @param hadoopConfig The Hadoop configuration to use for saving the data to HDFS
    * @param trainable    Whether the saved matrix should be retrainable, requiring more data being saved
    * @param ec           The implicit execution context in which to execute the request
    * @return A future whether the matrix was successfully saved
    */
  def save(hdfsPath: String, hadoopConfig: Configuration, trainable: Boolean)
          (implicit ec: ExecutionContext): Future[Boolean]

  /**
    * Computes the dot products to be used as gradient updates
    * for the input and output word as well as the input and random negative words combinations
    *
    * @param iUser The user feature indices
    * @param wUser The user feature weights
    * @param iItem The item feature indices
    * @param wItem The item feature weights
    * @return The partial dot products
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the dot products and the cache keys for the adjust operation
    */
  def dotprod(iUser: Array[Array[Int]],
              wUser: Array[Array[Float]],
              iItem: Array[Array[Int]],
              wItem: Array[Array[Float]])(implicit ec: ExecutionContext): Future[(Array[Float], Array[Int])]

  /**
    * Adjusts the weights according to the received gradient updates
    *
    * @param g The general BPR gradient per training instance in the batch
    * @param cacheKeys The keys to retrieve the cached indices and weights
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing either the success or failure of the operation
    */
  def adjust(g: Array[Float], cacheKeys: Array[Int])(implicit ec: ExecutionContext): Future[Boolean]
}
