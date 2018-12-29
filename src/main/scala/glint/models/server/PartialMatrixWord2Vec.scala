package glint.models.server

import glint.messages.server.request._
import glint.messages.server.response.{ResponseDotProd, ResponseFloat, ResponseRowsFloat}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import spire.implicits.cforRange

import scala.util.Random

/**
  * A partial matrix holding floats and supporting specific messages for efficient distributed Word2Vec computation
  *
  * @param partition The partition
  * @param vectorSize The (full) vector size
  * @param aggregate The type of aggregation to apply
  * @param vocabCns The array of all word counts
  * @param n The number of negative examples to create per output word
  * @param unigramTableSize The size of the unigram table for efficient generation of random negative words.
  *                         Smaller sizes can prevent OutOfMemoryError but might lead to worse results
  */
private[glint] class PartialMatrixWord2Vec(partition: Partition,
                                           vectorSize: Int,
                                           aggregate: Aggregate,
                                           val vocabCns: Array[Int],
                                           val n: Int,
                                           val unigramTableSize: Int = 100000000)
  extends PartialMatrixFloat(partition, vocabCns.length, partition.size, aggregate) {

  /**
    * The random number generator used for initializing the input weights matrix
    */
  val random = new Random(partition.index)

  /**
    * The input weights matrix
    */
  val u: Array[Float] = Array.fill(rows * cols)((random.nextFloat() - 0.5f) / vectorSize)

  /**
    * The output weights matrix
    */
  val v: Array[Float] = new Array(rows * cols)

  override val data: Array[Float] = u

  /**
    * The unigram table for efficient generation of random negative words
    */
  val table: Array[Int] = unigramTable()


  override def receive: Receive = {
    case pull: PullMatrix => sender ! ResponseFloat(get(pull.rows, pull.cols))
    case pull: PullMatrixRows => sender ! ResponseRowsFloat(getRows(pull.rows), cols)
    case push: PushMatrixFloat =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case pull: PullDotProd =>
      val (fPlus, fMinus) = dotprod(pull.wInput, pull.wOutput, pull.seed)
      sender ! ResponseDotProd(fPlus, fMinus)
    case push: PushAdjust =>
      adjust(push.wInput, push.wOutput, push.gPlus, push.gMinus, push.seed)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

  /**
    * Creates a unigram table for efficient generation of random negative words.
    *
    * @return The unigram table
    */
  private def unigramTable(): Array[Int] = {

    var trainWordsPow = 0.0
    val power = 0.75

    val table = new Array[Int](unigramTableSize)

    cforRange(vocabCns.indices)(i => {
      trainWordsPow += Math.pow(vocabCns(i), power)
    })
    var i = 0
    var d1 = Math.pow(vocabCns(i), power) / trainWordsPow
    cforRange(0 until unigramTableSize)(a => {
      table(a) = i
      if (a / unigramTableSize.toDouble > d1) {
        i += 1
        d1 += Math.pow(vocabCns(i), power) / trainWordsPow
      }
      if (i >= vocabCns.length) {
        i = vocabCns.length - 1
      }
    })

    table
  }

  /**
    * Gets random negative words
    *
    * @param random The random number generator used for generating negative words
    * @param wOut The index of the output word which should not be among the random examples
    * @return The negative words
    */
  private def negativeExamples(random: Random, wOut: Int): Array[Int] = {
    Array.fill[Int](n) {
      var nextRandom = random.nextInt(unigramTableSize)
      var nOut = table(nextRandom)
      while (nOut == wOut) {
        nextRandom = random.nextInt(unigramTableSize)
        nOut = table(nextRandom)
      }
      if (nOut == 0) {
        nOut = nextRandom % (vocabCns.length - 1) + 1
      }
      nOut
    }
  }

  /**
    * Computes the partial dot products to be used as partial gradient updates
    * for the input and output word as well as the input and random negative words combinations
    *
    * @param wInput The indices of the input words
    * @param wOutput The indices of the output words per input word
    * @param seed The seed for generating random negative words
    * @return The gradient updates
    */
  def dotprod(wInput: Array[Int], wOutput: Array[Array[Int]], seed: Long): (Array[Float], Array[Float]) = {

    val random = new Random(seed)

    val length = wOutput.map(_.length).sum
    val fPlus = new Array[Float](length)
    val fMinus = new Array[Float](length * n)

    var pos = 0
    var neg = 0

    cforRange(wInput.indices)(i => {
      val wIn = wInput(i)
      cforRange(wOutput(i).indices)(j => {
        val wOut = wOutput(i)(j)

        // generate n random negative examples for wOut
        val nOutput = negativeExamples(random, wOut)

        // compute partial dot products for positive and negative words
        fPlus(pos) = blas.sdot(cols, u, wIn * cols, 1, v, wOut * cols, 1)
        pos += 1
        cforRange(nOutput.indices)(k => {
          val nOut = nOutput(k)
          fMinus(neg) = blas.sdot(cols, u, wIn * cols, 1, v, nOut * cols, 1)
          neg += 1
        })
      })
    })

    (fPlus, fMinus)
  }

  /**
    * Adjusts the weights according to the received partial gradient updates
    * for the input and output word as well as the input and random negative words combinations
    *
    * @param wInput The indices of the input words
    * @param wOutput The indices of the output words per input word
    * @param gPlus The gradient updates for the input and output word combinations
    * @param gMinus The gradient updates for the input and random negative word combinations
    * @param seed The same seed that was used for generating random negative words for the partial dot products
    */
  def adjust(wInput: Array[Int],
             wOutput: Array[Array[Int]],
             gPlus: Array[Float],
             gMinus: Array[Float],
             seed: Long): Unit = {

    val random = new Random(seed)

    var pos = 0
    var neg = 0

    // matrices holding partial gradient updates to be applied at the end
    // only create arrays on demand for rows which are updated
    val u_updates = Array.ofDim[Float](rows, 0)
    val v_updates = Array.ofDim[Float](rows, 0)

    cforRange(wInput.indices)(i => {
      val wIn = wInput(i)
      if (u_updates(wIn).isEmpty) {
        u_updates(wIn) = new Array[Float](cols)
      }

      cforRange(wOutput(i).indices)(j => {
        val wOut = wOutput(i)(j)
        if (v_updates(wOut).isEmpty) {
          v_updates(wOut) = new Array[Float](cols)
        }

        // add partial gradient updates for positive word
        blas.saxpy(cols, gPlus(pos), v, wOut * cols, 1, u_updates(wIn), 0, 1)
        blas.saxpy(cols, gPlus(pos), u, wIn * cols, 1, v_updates(wOut), 0, 1)
        pos += 1

        // generate n random negative examples for wOut
        val nOutput = negativeExamples(random, wOut)

        cforRange(nOutput.indices)(k => {
          val nOut = nOutput(k)
          if (v_updates(nOut).isEmpty) {
            v_updates(nOut) = new Array[Float](cols)
          }

          // add partial gradient updates for negative word
          blas.saxpy(cols, gMinus(neg), v, nOut, 1, u_updates(wIn), 0, 1)
          blas.saxpy(cols, gMinus(neg), u, wIn, 1, v_updates(nOut), 0, 1)
          neg += 1
        })
      })
    })

    // apply partial gradient updates if not already done immediately
    cforRange(u.indices)(i => {
      if (!u_updates(i).isEmpty) {
        blas.saxpy(cols, 1.0f, u_updates(i), 0, 1, u, i, 1)
      }
      if (!v_updates(i).isEmpty) {
        blas.saxpy(cols, 1.0f, v_updates(i), 0, 1, v, i, 1)
      }
    })
  }

}

