package glint.models.client.granular

import glint.models.client.BigWord2VecMatrix

import scala.concurrent.{ExecutionContext, Future}

/**
  * A [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]] whose messages are limited to a specific maximum
  * message size. This helps resolve timeout exceptions and heartbeat failures in akka at the cost of additional message
  * overhead. The specific parameter server operations for efficient distributed Word2Vec computation are not limited to
  * a specific maximum message size.
  *
  * @param underlying The underlying big matrix
  * @param maximumMessageSize The maximum message size
  */
class GranularBigWord2VecMatrix(underlying: BigWord2VecMatrix, maximumMessageSize: Int)
  extends GranularBigMatrix[Float](underlying, maximumMessageSize) with BigWord2VecMatrix {

  override def dotprod(wInput: Array[Int], wOutput: Array[Array[Int]], seed: Long)
                      (implicit ec: ExecutionContext): Future[(Array[Float], Array[Float])] = {
    underlying.dotprod(wInput, wOutput, seed)
  }

  override def adjust(wInput: Array[Int],
                      wOutput: Array[Array[Int]],
                      gPlus: Array[Float],
                      gMinus: Array[Float],
                      seed: Long)(implicit ec: ExecutionContext): Future[Boolean] = {
    underlying.adjust(wInput, wOutput, gPlus, gMinus, seed)
  }

  override def norms()(implicit ec: ExecutionContext): Future[Array[Float]] = {
    underlying.norms()
  }

  override def multiply(vector: Array[Float])(implicit ec: ExecutionContext): Future[Array[Float]] = {
    underlying.multiply(vector)
  }
}
