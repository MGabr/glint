package glint.models.client.async

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import akka.actor.{ActorRef, ExtendedActorSystem}
import akka.pattern.Patterns.gracefulStop
import akka.serialization.JavaSerializer
import breeze.linalg.{DenseVector, Vector}
import breeze.math.Semiring
import com.typesafe.config.Config
import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushSave}
import glint.models.client.BigMatrix
import glint.models.server.aggregate.Aggregate
import glint.partitioning.by.PartitionBy
import glint.partitioning.{Partition, Partitioner}
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import glint.util.hdfs.MatrixMetadata
import org.apache.hadoop.conf.Configuration
import spire.implicits.cforRange

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * An asynchronous implementation of a [[glint.models.client.BigMatrix BigMatrix]]. You don't want to construct this
  * object manually but instead use the methods provided in [[glint.Client Client]], as so
  * {{{
  *   client.matrix[Double](rows, columns)
  * }}}
  *
  * @param partitioner A partitioner to map rows to partitions
  * @param matrices The references to the partial matrices on the parameter servers
  * @param config The glint configuration (used for serialization/deserialization construction of actorsystems)
  * @param aggregate The type of aggregation to perform on this model (used only for saving this parameter)
  * @param rows The number of rows
  * @param cols The number of columns
  * @tparam V The type of values to store
  * @tparam R The type of responses we expect to get from the parameter servers
  * @tparam P The type of push requests we should send to the parameter servers
  */
abstract class AsyncBigMatrix[@specialized V: Semiring : ClassTag, R: ClassTag, P: ClassTag](partitioner: Partitioner,
                                                                                             matrices: Array[ActorRef],
                                                                                             config: Config,
                                                                                             aggregate: Aggregate,
                                                                                             val rows: Long,
                                                                                             val cols: Long)
  extends BigMatrix[V] {

  require(
    if (partitioner.partitionBy == PartitionBy.ROW) cols <= Int.MaxValue else rows <= Int.MaxValue,
    "The number of non-keys has to be an Int")

  PullFSM.initialize(config)
  PushFSM.initialize(config)

  /**
    * Pulls a set of rows
    *
    * @param rows The indices of the rows
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the vectors representing the rows
    */
  override def pull(rows: Array[Long])(implicit ec: ExecutionContext): Future[Array[Vector[V]]] = {

    if (partitioner.partitionBy != PartitionBy.ROW) {
      return Future.failed(new UnsupportedOperationException("Cannot pull rows if the matrix is not partitioned by rows"))
    }

    // Send pull request of the list of keys
    val pulls = rows.groupBy(partitioner.partition).map {
      case (partition, partitionKeys) =>
        val pullMessage = PullMatrixRows(partitionKeys)
        val fsm = PullFSM[PullMatrixRows, R](pullMessage, matrices(partition.index))
        fsm.run()
    }

    // Obtain key indices after partitioning so we can place the results in a correctly ordered array
    val indices = rows.zipWithIndex.groupBy {
      case (k, i) => partitioner.partition(k)
    }.map {
      case (_, arr) => arr.map(_._2)
    }.toArray

    // Define aggregator for successful responses
    def aggregateSuccess(responses: Iterable[R]): Array[Vector[V]] = {
      val result: Array[Vector[V]] = Array.fill[Vector[V]](rows.length)(DenseVector.zeros[V](0))
      val responsesArray = responses.toArray
      val colsInt = cols.toInt
      cforRange(0 until responsesArray.length)(i => {
        val response = responsesArray(i)
        val idx = indices(i)
        cforRange(0 until idx.length)(i => {
          result(idx(i)) = toVector(response, i * colsInt, (i + 1) * colsInt)
        })
      })
      result
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)
  }

  /**
    * Pulls a set of elements
    *
    * @param rows The indices of the rows
    * @param cols The corresponding indices of the columns
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing the values of the elements at given rows, columns
    */
  override def pull(rows: Array[Long], cols: Array[Long])(implicit ec: ExecutionContext): Future[Array[V]] = {

    // Send pull request of the list of keys
    val pulls = mapPartitions(keys(rows, cols)) {
      case (partition, indices) =>
        val pullMessage = PullMatrix(indices.map(rows).toArray, indices.map(cols).toArray)
        val fsm = PullFSM[PullMatrix, R](pullMessage, matrices(partition.index))
        fsm.run()
    }

    // Obtain key indices after partitioning so we can place the results in a correctly ordered array
    val indices = keys(rows, cols).zipWithIndex.groupBy {
      case (k, i) => partitioner.partition(k)
    }.map {
      case (_, arr) => arr.map(_._2)
    }.toArray

    // Define aggregator for successful responses
    def aggregateSuccess(responses: Iterable[R]): Array[V] = {
      val responsesArray = responses.toArray
      val result = new Array[V](rows.length)
      cforRange(0 until responsesArray.length)(i => {
        val response = responsesArray(i)
        val idx = indices(i)
        cforRange(0 until idx.length)(j => {
          result(idx(j)) = toValue(response, j)
        })
      })
      result
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)

  }

  /**
    * Pushes a set of values
    *
    * @param rows The indices of the rows
    * @param cols The indices of the columns
    * @param values The values to update
    * @param ec The implicit execution context in which to execute the request
    * @return A future containing either the success or failure of the operation
    */
  override def push(rows: Array[Long], cols: Array[Long], values: Array[V])(implicit ec: ExecutionContext): Future[Boolean] = {

    // Send push requests
    val pushes = mapPartitions(keys(rows, cols)) {
      case (partition, indices) =>
        val rs = indices.map(rows).toArray
        val cs = indices.map(cols).toArray
        val vs = indices.map(values).toArray
        val fsm = PushFSM[P](id => toPushMessage(id, rs, cs, vs), matrices(partition.index))
        fsm.run()
    }

    // Combine and aggregate futures
    Future.sequence(pushes).transform(results => true, err => err)

  }

  /**
    * Returns the indices of the rows or columns depending on which are the partitioning keys
    *
    * @param rows The indices of the rows
    * @param cols The indices of the columns
    * @return The partitioning keys
    */
  private def keys(rows: Array[Long], cols: Array[Long]): Array[Long] = {
    if (partitioner.partitionBy == PartitionBy.ROW) rows else cols
  }

  /**
    * Groups indices of given keys into partitions according to this models partitioner and maps each partition to a
    * type T
    *
    * @param keys The keys to partition
    * @param func The function that takes a partition and corresponding indices and creates something of type T
    * @tparam T The type to map to
    * @return An iterable over the partitioned results
    */
  @inline
  protected def mapPartitions[T](keys: Seq[Long])(func: (Partition, Seq[Int]) => T): Iterable[T] = {
    keys.indices.groupBy(i => partitioner.partition(keys(i))).map { case (a, b) => func(a, b) }
  }

  /**
    * Saves the matrix to HDFS
    *
    * @param hdfsPath The HDFS base path where the matrix should be saved
    * @param hadoopConfig The Hadoop configuration to use for saving the data to HDFS
    * @param ec The implicit execution context in which to execute the request
    * @return A future whether the matrix was successfully saved
    */
  override def save(hdfsPath: String, hadoopConfig: Configuration)(implicit ec: ExecutionContext): Future[Boolean] = {
    val meta = MatrixMetadata(rows, cols, aggregate, partitioner.partitionBy, (_, _) => partitioner)
    hdfs.saveMatrixMetadata(hdfsPath, hadoopConfig, meta)

    val serHadoopConfig = new SerializableHadoopConfiguration(hadoopConfig)

    val pushes = partitioner.all().map {
      case partition =>
        val fsm = PushFSM[PushSave](id => PushSave(id, hdfsPath, serHadoopConfig), matrices(partition.index))
        fsm.run()
    }.toIterator
    Future.sequence(pushes).transform(results => true, err => err)
  }

  /**
    * Destroys the matrix on the parameter servers
    *
    * @return A future whether the matrix was successfully destroyed
    */
  override def destroy()(implicit ec: ExecutionContext): Future[Boolean] = {
    val partitionFutures = partitioner.all().map {
      case partition => gracefulStop(matrices(partition.index), 60 seconds)
    }.toIterator
    Future.sequence(partitionFutures).transform(successes => successes.forall(success => success), err => err)
  }

  /**
    * @return The number of partitions this big matrix's data is spread across
    */
  def nrOfPartitions: Int = {
    partitioner.all().length
  }

  /**
    * Creates a vector from range [start, end) in values in given response
    *
    * @param response The response containing the values
    * @param start The start index
    * @param end The end index
    * @return A vector for the range [start, end)
    */
  @inline
  protected def toVector(response: R, start: Int, end: Int): Vector[V]

  /**
    * Extracts a value from a given response at given index
    *
    * @param response The response
    * @param index The index
    * @return The value
    */
  @inline
  protected def toValue(response: R, index: Int): V

  /**
    * Creates a push message from given sequence of rows, columns and values
    *
    * @param id The identifier
    * @param rows The rows
    * @param cols The columns
    * @param values The values
    * @return A PushMatrix message for type V
    */
  @inline
  protected def toPushMessage(id: Int, rows: Array[Long], cols: Array[Long], values: Array[V]): P

  /**
    * Deserializes this instance. This starts an ActorSystem with appropriate configuration before attempting to
    * deserialize ActorRefs
    *
    * @param in The object input stream
    * @throws java.io.IOException A possible Input/Output exception
    */
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    val config = in.readObject().asInstanceOf[Config]
    val as = DeserializationHelper.getActorSystem(config.getConfig("glint.client"))
    JavaSerializer.currentSystem.withValue(as.asInstanceOf[ExtendedActorSystem]) {
      in.defaultReadObject()
    }
  }

  /**
    * Serializes this instance. This first writes the config before the entire object to ensure we can deserialize with
    * an ActorSystem in place
    *
    * @param out The object output stream
    * @throws java.io.IOException A possible Input/Output exception
    */
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(config)
    out.defaultWriteObject()
  }

}
