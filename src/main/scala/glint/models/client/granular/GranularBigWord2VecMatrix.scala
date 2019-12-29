package glint.models.client.granular

import breeze.linalg.Vector
import glint.models.client.BigWord2VecMatrix
import org.apache.hadoop.conf.Configuration
import spire.implicits.cforRange

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
  * A [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]] whose messages are limited to a specific maximum
  * message size. This helps resolve timeout exceptions and heartbeat failures in akka at the cost of additional message
  * overhead.
  *
  * The specific parameter server operations for efficient distributed Word2Vec training are not limited to
  * a specific maximum message size. The operations for Word2Vec prediction are however.
  *
  * @param underlying The underlying big matrix
  * @param maximumMessageSize The maximum message size
  */
class GranularBigWord2VecMatrix(underlying: BigWord2VecMatrix, maximumMessageSize: Int)
  extends GranularBigMatrix[Float](underlying, maximumMessageSize) with BigWord2VecMatrix {

  /**
    * The number of partitions
    */
  private[glint] val numPartitions: Int = underlying.numPartitions

  /**
    * Saves the matrix to HDFS
    *
    * @param hdfsPath The HDFS base path where the matrix should be saved
    * @param hadoopConfig The Hadoop configuration to use for saving the data to HDFS
    * @param trainable Whether the saved matrix should be retrainable, requiring more data being saved
    * @param ec The implicit execution context in which to execute the request
    * @return A future whether the matrix was successfully saved
    */
  override def save(hdfsPath: String, hadoopConfig: Configuration, trainable: Boolean)
                   (implicit ec: ExecutionContext): Future[Boolean] = {
    underlying.save(hdfsPath, hadoopConfig, trainable)
  }

  /**
    * Computes the dot products to be used as gradient updates
    * for the input and output word as well as the input and random negative words combinations.
    *
    * Makes no attempt to keep individual network messages smaller than `maximumMessageSize`!
    *
    * @param wInput The indices of the input words
    * @param wOutput The indices of the output words per input word
    * @param seed The seed for generating random negative words
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the gradient updates
    */
  override def dotprod(wInput: Array[Int], wOutput: Array[Array[Int]], seed: Long)
                      (implicit ec: ExecutionContext): Future[(Array[Float], Array[Float], Array[Int])] = {
    underlying.dotprod(wInput, wOutput, seed)
  }

  /**
    * Adjusts the weights according to the received gradient updates
    * for the input and output word as well as the input and random negative words combinations.
    *
    * Makes no attempt to keep individual network messages smaller than `maximumMessageSize`!
    *
    * @param gPlus The gradient updates for the input and output word combinations
    * @param gMinus The gradient updates for the input and random neighbour word combinations
    * @param cacheKeys The keys to retrieve the cached indices and weights
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing either the success or failure of the operation
    */
  override def adjust(gPlus: Array[Float], gMinus: Array[Float], cacheKeys: Array[Int])
                     (implicit ec: ExecutionContext): Future[Boolean] = {
    underlying.adjust(gPlus, gMinus, cacheKeys)
  }

  /**
    * Pulls the euclidean norm of each input weight row
    * while keeping individual network messages smaller than `maximumMessageSize`
    *
    * @param startRow The start index of the range of rows whose euclidean norms to get
    * @param endRow The exclusive end index of the range of rows whose euclidean norms to get
    * @param ec The implicit execution context in which to execute the request
    * @return The euclidean norms
    */
  override def norms(startRow: Int = 0, endRow: Int = rows.toInt)
                    (implicit ec: ExecutionContext): Future[Array[Float]] = {
    pullStartToEndRow(underlying.norms, startRow, endRow)
  }

  /**
    * Pulls the result of the matrix multiplication of the input weight matrix with the received vector
    * while keeping individual network messages smaller than `maximumMessageSize`
    *
    * @param vector The vector with which to multiply the matrix
    * @param startRow The start row index of the matrix, to support multiplication with only a part of the matrix
    * @param endRow The exclusive end row index of the matrix
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the matrix multiplication result
    */
  override def multiply(vector: Array[Float], startRow: Int = 0, endRow: Int = rows.toInt)
                       (implicit ec: ExecutionContext): Future[Array[Float]] = {
    pullStartToEndRow((start, end) => underlying.multiply(vector, start, end), startRow, endRow)
  }

  private def pullStartToEndRow(pull: (Int, Int) => Future[Array[Float]], startRow: Int, endRow: Int)
                               (implicit ec: ExecutionContext): Future[Array[Float]] = {
    val rows = endRow - startRow
    if (rows  <= maximumMessageSize) {
      pull(startRow, endRow)
    } else {
      var i = startRow
      var current = 0
      val a = new Array[Future[Array[Float]]](Math.ceil(rows.toDouble / maximumMessageSize.toDouble).toInt)
      while (i < endRow) {
        val future = pull(i, Math.min(endRow, i + maximumMessageSize))
        a(current) = future
        current += 1
        i += maximumMessageSize
      }
      Future.sequence(a.toIterator).map {
        case arrayOfValues =>
          val finalValues = new ArrayBuffer[Float](rows)
          arrayOfValues.foreach(x => finalValues.appendAll(x))
          finalValues.toArray
      }
    }
  }

  /**
    * Pulls the average of each set of rows while keeping individual network messages smaller than `maximumMessageSize`
    *
    * @param rows The array of row indices to average
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the average vectors
    */
  override def pullAverage(rows: Array[Array[Int]])(implicit ec: ExecutionContext): Future[Array[Vector[Float]]] = {

    // message sizes are limited by either the number of rows to average or the columns of the average vector
    val partialCols = Math.ceil(underlying.cols.toDouble / underlying.numPartitions.toDouble).toInt
    val maxLengths = rows.map(_.length).map(l => if (l > partialCols) l else partialCols)

    if (maxLengths.sum <= maximumMessageSize) {
      underlying.pullAverage(rows)
    } else {
      var current = 0
      var length = 0
      var futures = Seq[Future[Array[Vector[Float]]]]()
      cforRange (0 until rows.length) { i =>
        val maxLength = maxLengths(i)
        if (length + maxLength < maximumMessageSize) {
          length += maxLength
        } else {
          futures = futures :+ underlying.pullAverage(rows.slice(current, i))
          current = i
          length = maxLength
        }
      }
      if (current < rows.length) {
        futures = futures :+ underlying.pullAverage(rows.slice(current, rows.length))
      }
      Future.sequence(futures).map {
        case arrayOfVectors =>
          val finalArray = new ArrayBuffer[Vector[Float]](rows.length)
          arrayOfVectors.foreach(x => finalArray.appendAll(x))
          finalArray.toArray
      }
    }
  }
}
