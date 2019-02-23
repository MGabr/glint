package glint.models.client

import breeze.linalg.Vector

import scala.concurrent.{ExecutionContext, Future}

/**
  * A big matrix of floats and supporting specific parameter server operations
  * for efficient distributed Word2Vec computation
  *
  * The pull and push operations can only access the input weights of the underlying weight matrix,
  * the output weights stay hidden
  *
  * {{{
  *   val matrix: BigWord2VecMatrix = ...
  *   val wInput = Array(0L, 1L) // input word indices
  *   val wOutput = Array(Array(0L, 1L), Array(0L, 1L)) // output word indices
  *   val (fPlus, fMinus) = matrix.dotprod(wInput, wOutput, 1) // compute dot products for gradient updates
  *   matrix.adjust(wInput, wOutput, fPlus, gMinus, 1) // adjust matrix by gradient updates
  *   matrix.destroy() // Destroy matrix, freeing up memory on the parameter server
  * }}}
  */
trait BigWord2VecMatrix extends BigMatrix[Float] {

  /**
    * Computes the dot products to be used as gradient updates
    * for the input and output word as well as the input and random negative words combinations
    *
    * @param wInput The indices of the input words
    * @param wOutput The indices of the output words per input word
    * @param seed The seed for generating random negative words
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the gradient updates
    */
  def dotprod(wInput: Array[Int], wOutput: Array[Array[Int]], seed: Long)
             (implicit ec: ExecutionContext): Future[(Array[Float], Array[Float])]

  /**
    * Adjusts the weights according to the received gradient updates
    * for the input and output word as well as the input and random negative words combinations
    *
    * @param wInput The indices of the input words
    * @param wOutput The indices of the output words per input word
    * @param gPlus The gradient updates for the input and output word combinations
    * @param gMinus The gradient updates for the input and random neighbour word combinations
    * @param seed The same seed that was used for generating random negative words for the dot products
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing either the success or failure of the operation
    */
  def adjust(wInput: Array[Int],
             wOutput: Array[Array[Int]],
             gPlus: Array[Float],
             gMinus: Array[Float],
             seed: Long)(implicit ec: ExecutionContext): Future[Boolean]

  /**
    * Pulls the euclidean norm of each input weight vector
    *
    * @param ec The implicit execution context in which to execute the request
    * @return The euclidean norms
    */
  def norms()(implicit ec: ExecutionContext): Future[Array[Float]]

  /**
    * Pulls the result of the matrix multiplication of the input weight matrix with the received vector
    *
    * @param vector The vector with which to multiply the matrix
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the matrix multiplication result
    */
  def multiply(vector: Array[Float])(implicit ec: ExecutionContext): Future[Array[Float]]

  /**
    * Pulls the average of a set of rows
    *
    * @param rows The indices of the rows
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the average vector
    */
  def pullAverage(rows: Array[Long])(implicit ec: ExecutionContext): Future[Vector[Float]]
}
