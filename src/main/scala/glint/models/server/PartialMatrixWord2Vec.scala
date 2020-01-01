package glint.models.server

import akka.pattern.pipe
import com.github.fommil.netlib.F2jBLAS
import glint.Word2VecArguments
import glint.messages.server.request._
import glint.messages.server.response.{ResponseDotProd, ResponseFloat, ResponseRowsFloat}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs.Word2VecMatrixMetadata
import glint.util.{FloatArrayPool, hdfs}
import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap
import spire.implicits.cforRange

import scala.concurrent.Future
import scala.util.Random

/**
  * A partial matrix holding floats and supporting specific messages for efficient distributed Word2Vec computation
  *
  * @param partition The partition
  * @param aggregate The type of aggregation to apply
  * @param hdfsPath The HDFS base path from which the partial matrix' initial data should be loaded from
  * @param hadoopConfig The serializable Hadoop configuration to use for loading the initial data from HDFS
  * @param args The [[glint.Word2VecArguments Word2VecArguments]]
  * @param vocabSize The vocabulary size
  * @param vocabCnsOpt The array of all word counts, if not specified it is loaded from HDFS
  * @param loadVocabCnOnly If only vocabCns should be loaded from HDFS (and not all initial data)
  * @param trainable Whether the matrix is trainable, requiring more data (output weights, unigram table)
  */
private[glint] class PartialMatrixWord2Vec(partition: Partition,
                                           aggregate: Aggregate,
                                           hdfsPath: Option[String],
                                           hadoopConfig: Option[SerializableHadoopConfiguration],
                                           val args: Word2VecArguments,
                                           val vocabSize: Int,
                                           val vocabCnsOpt: Option[Array[Int]],
                                           val loadVocabCnOnly: Boolean,
                                           val trainable: Boolean)
  extends PartialMatrixFloat(partition, vocabSize, partition.size, aggregate, hdfsPath, hadoopConfig) {

  @transient
  private lazy val blas = new F2jBLAS

  /**
    * The random number generator used for initializing the input weights matrix
    */
  val random = new Random(partition.index)

  /**
    * The input weights matrix
    */
  var u: Array[Float] = _

  /**
    * The output weights matrix
    */
  var v: Array[Float] = _

  /**
    * The array of all word counts
    */
  var vocabCns: Array[Int] = _

  /**
    * The unigram table for efficient generation of random negative words
    */
  var table: Array[Int] = _


  override def loadOrInitialize(initialize: => Array[Float], pathPostfix: String): Array[Float] = {
    if (loadVocabCnOnly) initialize else super.loadOrInitialize(initialize, pathPostfix)
  }

  override def preStart(): Unit = {
    u = loadOrInitialize(Array.fill(rows * cols)((random.nextFloat() - 0.5f) / args.vectorSize),
      pathPostfix = "/glint/data/u/")
    data = u

    if (trainable) {
      vocabCns = vocabCnsOpt.getOrElse(hdfs.loadWord2VecMatrixMetadata(hdfsPath.get, hadoopConfig.get.get()).vocabCns)
      table = unigramTable()

      v = loadOrInitialize(new Array(rows * cols), pathPostfix = "/glint/data/v/")
    }
  }


  /**
    * Array pools and maps used to prevent garbage collection.
    * Thread local so that they can be used in asynchronously handled messages
    */
  private val threadLocalUpdatesArrayPool = new ThreadLocal[FloatArrayPool] {
    override def initialValue(): FloatArrayPool = new FloatArrayPool(cols)
  }

  private val threadLocalUUpdatesMap = new ThreadLocal[IntObjectHashMap[Array[Float]]] {
    override def initialValue() = new IntObjectHashMap[Array[Float]](args.batchSize * args.window * (args.n + 1) * 10)
  }

  private val threadLocalVUpdatesMap = new ThreadLocal[IntObjectHashMap[Array[Float]]] {
    override def initialValue() = new IntObjectHashMap[Array[Float]](args.batchSize * args.window * (args.n + 1) * 10)
  }


  /**
   * Update procedures which updates the weights according to the supplied partial gradient updates.
   *
   * They are defined as thread local implementations to avoid the creation of an anonymous implementation
   * on each method call.
   */
  private val threadLocalUUpdateProcedure = new ThreadLocal[IntObjectProcedure[Array[Float]]] {
    override def initialValue(): IntObjectProcedure[Array[Float]] = new IntObjectProcedure[Array[Float]] {

      private val updatesArrayPool = threadLocalUpdatesArrayPool.get()

      override def value(i: Int, uUpdatesI: Array[Float]): Unit = {
        blas.saxpy(cols, 1.0f, uUpdatesI, 0, 1, u, i * cols, 1)
        updatesArrayPool.putClear(uUpdatesI)
      }
    }
  }

  private val threadLocalVUpdateProcedure = new ThreadLocal[IntObjectProcedure[Array[Float]]] {
    override def initialValue(): IntObjectProcedure[Array[Float]] = new IntObjectProcedure[Array[Float]] {

      private val updatesArrayPool = threadLocalUpdatesArrayPool.get()

      override def value(i: Int, vUpdatesI: Array[Float]): Unit = {
        blas.saxpy(cols, 1.0f, vUpdatesI, 0, 1, v, i * cols, 1)
        updatesArrayPool.putClear(vUpdatesI)
      }
    }
  }


  /**
   * Cache to avoid duplicate transmission of indices and weights
   */
  private val cacheDotProd = new ConcurrentHashMap[Int, (Array[Int], Array[Array[Int]], Long)]()

  private var lastCacheKey = 0


  /**
    * Receives and handles incoming messages
    *
    * The specific messages for efficient distributed Word2Vec training (dotprod and adjust) are executed
    * asynchronously and then send their result back to the sender. This means that we lose the Akka concurrency
    * guarantees and the dotprod and adjust methods access the actors state as shared mutable state
    * without any synchronization and locking. These methods can therefore overwrite each others gradient updates.
    *
    * This is, however, required to achieve good performance. As explained in papers like HOGWILD! this still achieves
    * a nearly optimal rate of convergence when the optimization problem is sparse.
    */
  override def receive: Receive = {
    case pull: PullMatrix =>
      sender ! ResponseFloat(get(pull.rows, pull.cols))
    case pull: PullMatrixRows =>
      sender ! ResponseRowsFloat(getRows(pull.rows), cols)
    case push: PushMatrixFloat =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case push: PushSaveTrainable =>
      save(push.path, push.hadoopConfig, push.trainable)
      updateFinished(push.id)
    case pull: PullDotProd =>
      val cacheKey = lastCacheKey
      lastCacheKey += 1
      Future {
        val (fPlus, fMinus) = dotprod(pull.wInput, pull.wOutput, pull.seed, cacheKey)
        ResponseDotProd(fPlus, fMinus, cacheKey)
      } pipeTo sender()
    case push: PushAdjust =>
      Future {
        adjust(push.gPlus, push.gMinus, push.cacheKey)
        true
      } pipeTo sender()
    case pull: PullNormDots =>
      sender ! ResponseFloat(normDots(pull.startRow, pull.endRow))
    case pull: PullMultiply =>
      sender ! ResponseFloat(multiply(pull.vector, pull.startRow, pull.endRow))
    case pull: PullAverageRows =>
      sender ! ResponseFloat(pullAverage(pull.rows))
    case x =>
      handleLogic(x, sender)
  }

  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration, saveTrainable: Boolean): Unit = {

    // the partial matrix holding the first partition also saves metadata
    if (partition.index == 0) {
      val meta = Word2VecMatrixMetadata(vocabCns, args, trainable && saveTrainable)
      hdfs.saveWord2VecMatrixMetadata(hdfsPath, hadoopConfig.conf, meta)
    }

    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, u, pathPostfix = "/glint/data/u/")
    if (trainable && saveTrainable) {
      hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, v, pathPostfix = "/glint/data/v/")
    }
  }

  /**
    * Creates a unigram table for efficient generation of random negative words.
    * It has the same frequency distribution as the words from the corpus
    * and contains each word multiple times depending on the words frequency.
    *
    * @return The unigram table
    */
  private def unigramTable(): Array[Int] = {

    var trainWordsPow = 0.0
    val power = 0.75

    val table = new Array[Int](args.unigramTableSize)

    cforRange(0 until vocabSize)(i => {
      trainWordsPow += Math.pow(vocabCns(i), power)
    })
    var i = 0
    var d1 = Math.pow(vocabCns(i), power) / trainWordsPow
    cforRange(0 until args.unigramTableSize)(a => {
      table(a) = i
      if (a / args.unigramTableSize.toDouble > d1) {
        i += 1
        d1 += Math.pow(vocabCns(i), power) / trainWordsPow
      }
      if (i >= vocabSize) {
        i = vocabCns.length - 1
      }
    })

    table
  }

  /**
    * Get random negative word
    *
    * @param random The random number generator to use
    * @param wOut The index of the output word which should not be a random negative word
    * @return The negative word
    */
  @inline
  private def negativeExample(random: Random, wOut: Int): Int = {
    var nextRandom = random.nextInt(args.unigramTableSize)
    var nOut = table(nextRandom)
    while (nOut == wOut) {
      nextRandom = random.nextInt(args.unigramTableSize)
      nOut = table(nextRandom)
    }
    if (nOut == 0) {
      nOut = nextRandom % (vocabSize - 1) + 1
    }
    nOut
  }

  /**
    * Computes the partial dot products to be used as partial gradient updates
    * for the input and output word as well as the input and random negative words combinations
    *
    * @param wInput The indices of the input words
    * @param wOutput The indices of the output words per input word
    * @param seed The seed for generating random negative words
    * @param cacheKey The key to retrieve the cached indices and weights
    * @return The gradient updates
    */
  def dotprod(wInput: Array[Int],
              wOutput: Array[Array[Int]],
              seed: Long,
              cacheKey: Int): (Array[Float], Array[Float]) = {

    cacheDotProd.put(cacheKey, (wInput, wOutput, seed))

    val random = new Random(seed)

    val length = wOutput.map(_.length).sum
    val fPlus = new Array[Float](length)
    val fMinus = new Array[Float](length * args.n)

    var pos = 0
    var neg = 0

    cforRange(0 until wInput.length)(i => {
      val wIn = wInput(i)
      cforRange(0 until wOutput(i).length)(j => {
        val wOut = wOutput(i)(j)

        // compute partial dot product for positive word
        fPlus(pos) = blas.sdot(cols, u, wIn * cols, 1, v, wOut * cols, 1)
        pos += 1

        // compute partial dot products for n random negative words
        cforRange(0 until args.n)(k => {
          val nOut = negativeExample(random, wOut)
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
    * @param gPlus The gradient updates for the input and output word combinations
    * @param gMinus The gradient updates for the input and random negative word combinations
    * @param cacheKey The key to retrieve the cached indices and weights
    */
  def adjust(gPlus: Array[Float], gMinus: Array[Float], cacheKey: Int): Unit = {

    // for asynchronous exactly-once delivery with PullFSM
    if (!cacheDotProd.containsKey(cacheKey)) {
      return
    }
    val (wInput, wOutput, seed) = cacheDotProd.get(cacheKey)
    cacheDotProd.remove(cacheKey)

    val random = new Random(seed)

    var pos = 0
    var neg = 0

    val updatesArrayPool = threadLocalUpdatesArrayPool.get()

    val uUpdates = threadLocalUUpdatesMap.get()
    val vUpdates = threadLocalVUpdatesMap.get()

    cforRange(0 until wInput.length)(i => {
      val wIn = wInput(i)
      val uUpdatesWIn = uUpdates.getIfAbsentPut(wIn, updatesArrayPool.get())

      cforRange(0 until wOutput(i).length)(j => {
        val wOut = wOutput(i)(j)

        // add partial gradient updates for positive word
        blas.saxpy(cols, gPlus(pos), v, wOut * cols, 1, uUpdatesWIn, 0, 1)
        blas.saxpy(cols, gPlus(pos), u, wIn * cols, 1, vUpdates.getIfAbsentPut(wOut, updatesArrayPool.get()), 0, 1)
        pos += 1

        // add partial gradient updates for the same n random negative words
        cforRange(0 until args.n)(k => {
          val nOut = negativeExample(random, wOut)
          blas.saxpy(cols, gMinus(neg), v, nOut * cols, 1, uUpdatesWIn, 0, 1)
          blas.saxpy(cols, gMinus(neg), u, wIn * cols, 1, vUpdates.getIfAbsentPut(nOut, updatesArrayPool.get()), 0, 1)
          neg += 1
        })
      })
    })

    // apply partial gradient updates
    uUpdates.forEachKeyValue(threadLocalUUpdateProcedure.get())
    vUpdates.forEachKeyValue(threadLocalVUpdateProcedure.get())

    uUpdates.clear()
    vUpdates.clear()
  }

  /**
    * Pulls the partial dot products of each partial input weight vector with itself.
    * This can be used to then compute the euclidean norm on the client
    *
    * @param startRow The start index of the range of rows whose partial dot products to get
    * @param endRow The exclusive end index of the range of rows whose partial dot products to get
    * @return The partial dot products
    */
  def normDots(startRow: Int, endRow: Int): Array[Float] = {
    val results = new Array[Float](endRow - startRow)
    cforRange(startRow until endRow)(i => {
      results(i - startRow) = blas.sdot(cols, u, i * cols, 1, u, i * cols, 1)
    })
    results
  }

  /**
    * Pulls the result of the matrix multiplication of the partial input weight matrix with the received partial vector
    *
    * @param vector The partial vector with which to multiply the partial matrix
    * @param startRow The start row index of the matrix, to support multiplication with only part of the partial matrix
    * @param endRow The exclusive end row index of the matrix
    * @return The matrix multiplication result
    */
  def multiply(vector: Array[Float], startRow: Int, endRow: Int): Array[Float] = {
    val rows = endRow - startRow
    val resultVector = new Array[Float](rows)
    val alpha: Float = 1
    val beta: Float = 0
    blas.sgemv("T", cols, rows, alpha, u, startRow * cols, cols, vector, 0, 1, beta, resultVector, 0, 1)
    resultVector
  }

  /**
    * Pulls the partial average of each set of rows
    *
    * @param rows The indices of the rows
    * @return The average rows
    */
  def pullAverage(rows: Array[Array[Int]]): Array[Float] = {
    val result = new Array[Float](rows.length * cols)
    cforRange(0 until rows.length)(i => {
      val row = rows(i)
      cforRange(0 until row.length)(j => {
        blas.saxpy(cols, 1.0f, u, row(j) * cols, 1, result, i * cols, 1)
      })
      if (row.length != 0) {
        blas.sscal(cols, 1.0f / row.length, result, i * cols, 1)
      }
    })
    result
  }
}

