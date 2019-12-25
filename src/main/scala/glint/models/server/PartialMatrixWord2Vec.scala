package glint.models.server

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import akka.routing.{Group, Routee, Router}
import com.github.fommil.netlib.F2jBLAS
import glint.messages.server.logic.RouteesList
import glint.messages.server.request._
import glint.messages.server.response.{ResponseDotProd, ResponseFloat}
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs.Word2VecMatrixMetadata
import glint.util.{FloatArrayPool, hdfs}
import glint.{Server, Word2VecArguments}
import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap
import spire.implicits.cforRange

import scala.collection.immutable
import scala.util.Random

trait PartialMatrixWord2VecLogic {

  @transient
  private lazy val blas = new F2jBLAS

  val partitionId: Int
  val rows: Int
  val cols: Int
  val args: Word2VecArguments
  val trainable: Boolean

  var vocabCns: Array[Int]
  var u: Array[Float]
  var v: Array[Float]
  var table: Array[Int]

  /** Array pools and map used to prevent garbage collection */
  private val updatesArrayPool = new FloatArrayPool(cols)
  private val uUpdatesMap = new IntObjectHashMap[Array[Float]](args.batchSize * args.window * (args.n + 1) * 10)
  private val vUpdatesMap = new IntObjectHashMap[Array[Float]](args.batchSize * args.window * (args.n + 1) * 10)

  /** Cache to avoid duplicate transmission of indices and weights */
  private val cache = new IntObjectHashMap[(Array[Int], Array[Array[Int]], Long)]()

  /**
   * Update procedures which updates the weights according to the supplied partial gradient updates.
   *
   * Defined as variables to avoid the creation of an anonymous implementation on each method call.
   */
  private val uUpdateProcedure: IntObjectProcedure[Array[Float]] = new IntObjectProcedure[Array[Float]] {
    override def value(i: Int, uUpdatesI: Array[Float]): Unit = {
      blas.saxpy(cols, 1.0f, uUpdatesI, 0, 1, u, i * cols, 1)
      updatesArrayPool.putClear(uUpdatesI)
    }
  }
  private val vUpdateProcedure: IntObjectProcedure[Array[Float]] = new IntObjectProcedure[Array[Float]] {
    override def value(i: Int, vUpdatesI: Array[Float]): Unit = {
      blas.saxpy(cols, 1.0f, vUpdatesI, 0, 1, v, i * cols, 1)
      updatesArrayPool.putClear(vUpdatesI)
    }
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
      nOut = nextRandom % (rows - 1) + 1
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
   * @return The gradient updates
   */
  def dotprod(wInput: Array[Int],
              wOutput: Array[Array[Int]],
              seed: Long,
              cacheKey: Int): (Array[Float], Array[Float]) = {

    cache.put(cacheKey, (wInput, wOutput, seed))

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
   * @param cacheKey
   */
  def adjust(gPlus: Array[Float], gMinus: Array[Float], cacheKey: Int): Unit = {
    val (wInput, wOutput, seed) = cache.get(cacheKey)
    cache.remove(cacheKey)

    val random = new Random(seed)

    var pos = 0
    var neg = 0

    cforRange(0 until wInput.length)(i => {
      val wIn = wInput(i)
      val uUpdatesWIn = uUpdatesMap.getIfAbsentPut(wIn, updatesArrayPool.get())

      cforRange(0 until wOutput(i).length)(j => {
        val wOut = wOutput(i)(j)

        // add partial gradient updates for positive word
        blas.saxpy(cols, gPlus(pos), v, wOut * cols, 1, uUpdatesWIn, 0, 1)
        blas.saxpy(cols, gPlus(pos), u, wIn * cols, 1, vUpdatesMap.getIfAbsentPut(wOut, updatesArrayPool.get()), 0, 1)
        pos += 1

        // add partial gradient updates for the same n random negative words
        cforRange(0 until args.n)(k => {
          val nOut = negativeExample(random, wOut)
          blas.saxpy(cols, gMinus(neg), v, nOut * cols, 1, uUpdatesWIn, 0, 1)
          blas.saxpy(cols, gMinus(neg), u, wIn * cols, 1, vUpdatesMap.getIfAbsentPut(nOut, updatesArrayPool.get()), 0, 1)
          neg += 1
        })
      })
    })

    // apply partial gradient updates
    uUpdatesMap.forEachKeyValue(uUpdateProcedure)
    vUpdatesMap.forEachKeyValue(vUpdateProcedure)

    uUpdatesMap.clear()
    vUpdatesMap.clear()
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

  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration, saveTrainable: Boolean): Unit = {
    // the partial matrix holding the first partition also saves metadata
    if (partitionId == 0) {
      val meta = Word2VecMatrixMetadata(vocabCns, args, trainable && saveTrainable)
      hdfs.saveWord2VecMatrixMetadata(hdfsPath, hadoopConfig.conf, meta)
    }

    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, u, pathPostfix = "/glint/data/u/")
    if (trainable && saveTrainable) {
      hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, v, pathPostfix = "/glint/data/v/")
    }
  }
}

/**
  * A partial matrix holding floats and supporting specific messages for efficient distributed Word2Vec training
  */
private[glint] class PartialMatrixWord2Vec(partitionId: Int,
                                           cols: Int,
                                           hdfsPath: Option[String],
                                           hadoopConfig: Option[SerializableHadoopConfiguration],
                                           val args: Word2VecArguments,
                                           val vocabSize: Int,
                                           val vocabCnsOpt: Option[Array[Int]],
                                           val loadVocabCnOnly: Boolean,
                                           val trainable: Boolean)
  extends PartialMatrixFloat(partitionId, vocabSize, cols, hdfsPath, hadoopConfig) with PartialMatrixWord2VecLogic {

  /** The input weights matrix */
  var u: Array[Float] = _

  /** The output weights matrix */
  var v: Array[Float] = _

  /** The array of all word counts */
  var vocabCns: Array[Int] = _

  /** The unigram table for efficient generation of random negative words */
  var table: Array[Int] = _

  /** The routees which access the above matrices as shared mutable state */
  var routees: Array[ActorRef] = Array()

  override def loadOrInitialize(initialize: => Array[Float], pathPostfix: String): Array[Float] = {
    if (loadVocabCnOnly) initialize else super.loadOrInitialize(initialize, pathPostfix)
  }

  override def preStart(): Unit = {
    val random = new Random(partitionId)
    u = loadOrInitialize(Array.fill(rows * cols)((random.nextFloat() - 0.5f) / args.vectorSize),
      pathPostfix = "/glint/data/u/")
    data = u

    if (trainable) {
      vocabCns = vocabCnsOpt.getOrElse(hdfs.loadWord2VecMatrixMetadata(hdfsPath.get, hadoopConfig.get.get()).vocabCns)
      table = unigramTable()

      v = loadOrInitialize(new Array(rows * cols), pathPostfix = "/glint/data/v/")
    }

    routees = (0 until Server.cores).toArray.map(routeeId =>
      context.system.actorOf(Props(classOf[PartialMatrixWord2VecRoutee], routeeId, partitionId, rows, cols,
        args, trainable, vocabCns, u, v, table)))
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

  private def word2vecReceive: Receive = {
    case pull: PullDotProd =>
      val id = nextId()
      val (fPlus, fMinus) = dotprod(pull.wInput, pull.wOutput, pull.seed, id)
      sender ! ResponseDotProd(fPlus, fMinus, id)
    case push: PushAdjust =>
      adjust(push.gPlus, push.gMinus, push.id)
      updateFinished(push.id)
    case pull: PullNormDots =>
      sender ! ResponseFloat(normDots(pull.startRow, pull.endRow))
    case pull: PullMultiply =>
      sender ! ResponseFloat(multiply(pull.vector, pull.startRow, pull.endRow))
    case pull: PullAverageRows =>
      sender ! ResponseFloat(pullAverage(pull.rows))
    case push: PushSaveTrainable =>
      save(push.path, push.hadoopConfig, push.trainable)
      updateFinished(push.id)
    case RouteesList() =>
      sender ! routees
  }

  override def receive: Receive = word2vecReceive.orElse(super.receive)

  override def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration): Unit = {
    save(hdfsPath, hadoopConfig, true)
  }
}

/**
 * A routee actor which accesses the shared mutable state of a partial float matrix
 *
 * @param v: The shared mutable partial input weights matrix
 * @param u: The shared mutable partial output weights matrix
 */
private[glint] class PartialMatrixWord2VecRoutee(routeeId: Int,
                                                 partitionId: Int,
                                                 rows: Int,
                                                 cols: Int,
                                                 val args: Word2VecArguments,
                                                 val trainable: Boolean,
                                                 var vocabCns: Array[Int],
                                                 var u: Array[Float],
                                                 var v: Array[Float],
                                                 var table: Array[Int])
  extends PartialMatrixFloatRoutee(routeeId, partitionId, rows, cols, u) with PartialMatrixWord2VecLogic {

  private def word2vecReceive: Receive = {
    case pull: PullDotProd =>
      val id = nextId()
      val (fPlus, fMinus) = dotprod(pull.wInput, pull.wOutput, pull.seed, id)
      sender ! ResponseDotProd(fPlus, fMinus, id)
    case push: PushAdjust =>
      adjust(push.gPlus, push.gMinus, push.id)
      updateFinished(push.id)
    case pull: PullNormDots =>
      sender ! ResponseFloat(normDots(pull.startRow, pull.endRow))
    case pull: PullMultiply =>
      sender ! ResponseFloat(multiply(pull.vector, pull.startRow, pull.endRow))
    case pull: PullAverageRows =>
      sender ! ResponseFloat(pullAverage(pull.rows))
    case push: PushSaveTrainable =>
      save(push.path, push.hadoopConfig, push.trainable)
      updateFinished(push.id)
  }

  override def receive: Receive = word2vecReceive.orElse(super.receive)

  override def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration): Unit = {
    save(hdfsPath, hadoopConfig, true)
  }
}

/**
 * Routing logic for partial word2vec matrix routee actors
 */
private[glint] class PartialMatrixWord2VecRoutingLogic extends PartialMatrixFloatRoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case push: PushAdjust => routees(push.id >> 16)
      case push: PushSaveTrainable => routees(push.id >> 16)
      case _ => super.select(message, routees)
    }
  }
}

/**
 * A group router for partial word2vec matrix routee actors
 *
 * @param paths The paths of the routees
 */
private[glint] case class PartialMatrixWord2VecGroup(paths: immutable.Iterable[String]) extends Group {

  override def paths(system: ActorSystem): immutable.Iterable[String] = paths

  override def createRouter(system: ActorSystem): Router = new Router(new PartialMatrixWord2VecRoutingLogic)

  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId
}