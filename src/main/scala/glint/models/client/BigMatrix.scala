package glint.models.client

import breeze.linalg.Vector
import org.apache.hadoop.conf.Configuration

import scala.concurrent.{ExecutionContext, Future}

/**
  * A big matrix supporting basic parameter server row-wise and element-wise operations
  *
  * {{{
  *   val matrix: BigMatrix[Double] = ...
  *   matrix.pull(Array(0L, 1L, 2L)) // get full rows
  *   matrix.pull(Array(0L, 1L, 2L), Array(3, 100, 234)) // get values corresponding to row column indices
  *   matrix.push(Array(0L, 1L, 2L), Array(3, 100, 234), Array(0.5, 3.14, 9.9)) // add values to matrix
  *   matrix.destroy() // Destroy matrix, freeing up memory on the parameter server
  * }}}
  *
  * @tparam V The type of values to store
  */
trait BigMatrix[V] extends Serializable {

  /**
    * The number of rows
    */
  val rows: Long

  /**
    * The number of columns
    */
  val cols: Long

  /**
    * Pulls a set of rows
    *
    * @param rows The indices of the rows
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the vectors representing the rows
    */
  def pull(rows: Array[Long])(implicit ec: ExecutionContext): Future[Array[Vector[V]]]

  /**
    * Pulls a set of elements
    *
    * @param rows The indices of the rows
    * @param cols The corresponding indices of the columns
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the values of the elements at given rows, columns
    */
  def pull(rows: Array[Long], cols: Array[Long])(implicit ec: ExecutionContext): Future[Array[V]]

  /**
    * Pushes a set of values
    *
    * @param rows The indices of the rows
    * @param cols The indices of the columns
    * @param values The values to update
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing either the success or failure of the operation
    */
  def push(rows: Array[Long], cols: Array[Long], values: Array[V])(implicit ec: ExecutionContext): Future[Boolean]

  /**
    * Saves the matrix to HDFS
    *
    * @param hdfsPath The HDFS base path where the matrix should be saved
    * @param hadoopConfig The Hadoop configuration to use for saving the data to HDFS
    * @param ec The implicit execution context in which to execute the request
    * @return A future whether the matrix was successfully saved
    */
  def save(hdfsPath: String, hadoopConfig: Configuration)(implicit ec: ExecutionContext): Future[Boolean]

  /**
    * Destroys the big matrix and its resources on the parameter server
    *
    * @param ec The implicit execution context in which to execute the request
    * @return A future whether the matrix was successfully destroyed
    */
  def destroy()(implicit ec: ExecutionContext): Future[Boolean]
}
