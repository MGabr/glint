package glint

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.serialization.{Serialization, SerializationExtension}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import glint.exceptions.{ModelCreationException, ServerCreationException}
import glint.messages.master.{ClientList, RegisterClient, ServerList}
import glint.models.client.async._
import glint.models.client.{BigFMPairMatrix, BigFMPairVector, BigMatrix, BigVector, BigWord2VecMatrix}
import glint.models.server._
import glint.models.server.aggregate.{Aggregate, AggregateAdd}
import glint.partitioning.by.PartitionBy
import glint.partitioning.by.PartitionBy.PartitionBy
import glint.partitioning.range.RangePartitioner
import glint.partitioning.{Partition, Partitioner}
import glint.serialization.SerializableHadoopConfiguration
import glint.util._
import glint.util.hdfs.Word2VecMatrixMetadata
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.runtime.universe.TypeTag

/**
  * The client provides the functions needed to spawn large distributed matrices and vectors on the parameter servers.
  * Use the companion object to construct a Client object from a configuration file.
  *
  * @constructor Use the companion object to construct a Client object
  * @param config The configuration
  * @param system The actor system
  * @param master An actor reference to the master
  */
class Client(val config: Config,
             private[glint] val system: ActorSystem,
             private[glint] val master: ActorRef) {

  private implicit val timeout = Timeout(config.getDuration("glint.client.timeout", TimeUnit.MILLISECONDS) milliseconds)
  private implicit val ec = ExecutionContext.Implicits.global

  private[glint] val actor = system.actorOf(Props[ClientActor])
  private[glint] val registration = master ? RegisterClient(actor)

  /**
    * Creates a distributed model on the parameter servers
    *
    * @param keys The total number of keys
    * @param modelsPerServer The number of models to spawn per parameter server
    * @param createPartitioner A function that creates a partitioner based on a number of keys and partitions
    * @param generateServerProp A function that generates a server prop of a partial model for a particular partition
    * @param generateClientObject A function that generates a client object based on the partitioner and spawned models
    * @param reverseServers Whether models should be deployed to parameter servers in reverse order
    * @tparam M The final model type to generate
    * @return The generated model
    */
  private def create[M](keys: Long,
                        modelsPerServer: Int,
                        createPartitioner: (Int, Long) => Partitioner,
                        generateServerProp: Partition => Props,
                        generateClientObject: (Partitioner, Array[ActorRef], Config) => M,
                        reverseServers: Boolean = false): M = {

    // Get a list of servers
    val listOfServers = serverList()

    // Construct a big model based on the list of servers
    val bigModelFuture = listOfServers.map { servers =>

      // Check if there are servers online
      if (servers.isEmpty) {
        throw new ModelCreationException("Cannot create a model without active parameter servers")
      }

      if (reverseServers) servers.reverse else servers

    }.map { servers =>

      // Construct a partitioner
      var numberOfPartitions = Math.min(keys, modelsPerServer * servers.length).toInt
      val partitioner = createPartitioner(numberOfPartitions, keys)
      val partitions = partitioner.all()

      // Correct number of partitions in case we loaded a model with a different number of partitions
      if (partitions.length != numberOfPartitions) {
        numberOfPartitions = partitions.length
      }

      // Spawn models that are deployed on the parameter servers according to the partitioner
      val models = new Array[ActorRef](numberOfPartitions)
      var partitionIndex = 0
      while (partitionIndex < numberOfPartitions) {
        val serverIndex = partitionIndex % servers.length
        val server = servers(serverIndex)
        val partition = partitions(partitionIndex)
        val prop = generateServerProp(partition)
        models(partitionIndex) = system.actorOf(prop.withDeploy(Deploy(scope = RemoteScope(server.path.address))))
        partitionIndex += 1
      }

      // Construct a big model client object
      generateClientObject(partitioner, models, config)
    }

    // Wait for the big model to finish
    Await.result(bigModelFuture, config.getDuration("glint.client.timeout", TimeUnit.MILLISECONDS) milliseconds)

  }

  private def matrix[V: Numerical : TypeTag](rows: Long,
                                             cols: Long,
                                             modelsPerServer: Int,
                                             aggregate: Aggregate,
                                             partitionBy: PartitionBy,
                                             createPartitioner: (Int, Long) => Partitioner,
                                             hdfsPath: Option[String],
                                             hadoopConfig: Option[Configuration]): BigMatrix[V] = {

    val serHadoopConfig = hadoopConfig.map(new SerializableHadoopConfiguration(_))

    val propFunction = Numerical.asString[V] match {
      case "Int" => (partition: Partition) =>
        props(classOf[PartialMatrixInt], partition, rows, cols, aggregate, partitionBy, hdfsPath, serHadoopConfig)
      case "Long" => (partition: Partition) =>
        props(classOf[PartialMatrixLong], partition, rows, cols, aggregate, partitionBy, hdfsPath, serHadoopConfig)
      case "Float" => (partition: Partition) =>
        props(classOf[PartialMatrixFloat], partition, rows, cols, aggregate, partitionBy, hdfsPath, serHadoopConfig)
      case "Double" => (partition: Partition) =>
        props(classOf[PartialMatrixDouble], partition, rows, cols, aggregate, partitionBy, hdfsPath, serHadoopConfig)
      case x => throw new ModelCreationException(s"Cannot create model for unsupported value type $x")
    }

    val objFunction = Numerical.asString[V] match {
      case "Int" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigMatrixInt(partitioner, models, config, aggregate, rows, cols).asInstanceOf[BigMatrix[V]]
      case "Long" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigMatrixLong(partitioner, models, config, aggregate, rows, cols).asInstanceOf[BigMatrix[V]]
      case "Float" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigMatrixFloat(partitioner, models, config, aggregate, rows, cols).asInstanceOf[BigMatrix[V]]
      case "Double" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigMatrixDouble(partitioner, models, config, aggregate, rows, cols).asInstanceOf[BigMatrix[V]]
      case x => throw new ModelCreationException(s"Cannot create model for unsupported value type $x")
    }

    val keys = if (partitionBy == PartitionBy.ROW) rows else cols
    create[BigMatrix[V]](keys, modelsPerServer, createPartitioner, propFunction, objFunction)
  }

  private def props[T](matrixClass: Class[T],
                       partition: Partition,
                       rows: Long,
                       cols: Long,
                       aggregate: Aggregate,
                       partitionBy: PartitionBy,
                       hdfsPath: Option[String],
                       hadoopConfig: Option[SerializableHadoopConfiguration]): Props = {

    require(
      if (partitionBy == PartitionBy.ROW) cols <= Int.MaxValue else rows <= Int.MaxValue,
      "The number of non-keys has to be an Int")

    val rowsInt = if (partitionBy == PartitionBy.ROW) partition.size else rows.toInt
    val colsInt = if (partitionBy == PartitionBy.COL) partition.size else cols.toInt
    Props(matrixClass, partition, rowsInt, colsInt, aggregate, hdfsPath, hadoopConfig)
  }

  /**
    * Constructs a distributed matrix (indexed by (row: Long, col: Long)) for specified type of values
    *
    * @param rows The number of rows
    * @param cols The number of columns
    * @param modelsPerServer The number of partial models to store per parameter server (default: 1)
    * @param aggregate The type of aggregation to perform on this model (default: AggregateAdd)
    * @param partitionBy The key type for partitioning (row or column)
    * @tparam V The type of values to store, must be one of the following: Int, Long, Double or Float
    * @return The constructed [[glint.models.client.BigMatrix BigMatrix]]
    */
  def matrix[V: Numerical : TypeTag](rows: Long,
                                     cols: Long,
                                     modelsPerServer: Int = 1,
                                     aggregate: Aggregate = AggregateAdd(),
                                     partitionBy: PartitionBy = PartitionBy.ROW): BigMatrix[V] = {

    matrix[V](rows, cols, modelsPerServer, aggregate, partitionBy,
      (partitions: Int, keys: Long) => RangePartitioner(partitions, keys, partitionBy))
  }

  /**
    * Constructs a distributed matrix (indexed by (row: Long, col: Long)) for specified type of values
    *
    * @param rows The number of rows
    * @param cols The number of columns
    * @param modelsPerServer The number of partial models to store per parameter server
    * @param partitionBy The key type for partitioning (row or column)
    * @param createPartitioner A function that creates a [[glint.partitioning.Partitioner partitioner]] that partitions
    *                          keys
    * @tparam V The type of values to store, must be one of the following: Int, Long, Double or Float
    * @return The constructed [[glint.models.client.BigMatrix BigMatrix]]
    */
  def matrix[V: Numerical : TypeTag](rows: Long,
                                     cols: Long,
                                     modelsPerServer: Int,
                                     aggregate: Aggregate,
                                     partitionBy: PartitionBy,
                                     createPartitioner: (Int, Long) => Partitioner): BigMatrix[V] = {

    matrix(rows, cols, modelsPerServer, aggregate, partitionBy, createPartitioner, None, None)
  }

  /**
    * Loads a saved distributed matrix for specified type of values.
    * Keep in mind that there will be no error thrown when specifying a wrong type
    * but the loaded matrix will not work as intended.
    *
    * @param hdfsPath The HDFS base path from which the matrix' initial data should be loaded from
    * @param hadoopConfig The Hadoop configuration to use for loading the initial data from HDFS
    * @tparam V The type of values to store, must be one of the following: Int, Long, Double or Float
    * @return The constructed [[glint.models.client.BigMatrix BigMatrix]]
    */
  def loadMatrix[V: Numerical : TypeTag](hdfsPath: String, hadoopConfig: Configuration): BigMatrix[V] = {

    val m = hdfs.loadMatrixMetadata(hdfsPath, hadoopConfig)
    matrix(m.rows, m.cols, 1, m.aggregate, m.partitionBy, m.createPartitioner, Some(hdfsPath), Some(hadoopConfig))
  }

  private def word2vecMatrix(args: Word2VecArguments,
                             vocabSize: Int,
                             createPartitioner: (Int, Long) => Partitioner,
                             hdfsPath: String,
                             hadoopConfig: Configuration,
                             loadVocabCnOnly: Boolean,
                             trainable: Boolean): BigWord2VecMatrix = {

    val serHadoopConfig = new SerializableHadoopConfiguration(hadoopConfig)

    val propFunction = (partition: Partition) => Props(classOf[PartialMatrixWord2Vec], partition, AggregateAdd(),
      Some(hdfsPath), Some(serHadoopConfig), args, vocabSize, None, loadVocabCnOnly, trainable)

    val objFunction = (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
      new AsyncBigWord2VecMatrix(partitioner, models, config, AggregateAdd(), vocabSize, args.vectorSize, args.n,
        trainable)

    create[BigWord2VecMatrix](args.vectorSize, 1, createPartitioner, propFunction, objFunction)
  }

  /**
    * Constructs a Word2Vec matrix distributed over all available parameter servers
    *
    * This method for constructing the matrix has to save the vocabulary counts to a temporary HDFS file.
    * To efficiently construct a Word2Vec matrix in a standalone glint cluster in the same Spark application use
    * [[glint.Client.runWithWord2VecMatrixOnSpark(sc:org\.apache\.spark\.SparkContext)* runWithWord2VecMatrixOnSpark]]
    *
    * @param args The [[glint.Word2VecArguments Word2VecArguments]]
    * @param vocabCns The array of all word counts
    * @param hadoopConfig The Hadoop configuration to use for saving vocabCns to HDFS
    * @return The constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def word2vecMatrix(args: Word2VecArguments, vocabCns: Array[Int], hadoopConfig: Configuration): BigWord2VecMatrix = {

    val tmpPath = hdfs.saveTmpWord2VecMatrixMetadata(hadoopConfig, Word2VecMatrixMetadata(vocabCns, args, true))
    val createPartitioner = (partitions: Int, keys: Long) => RangePartitioner(partitions, keys, PartitionBy.COL)
    word2vecMatrix(args, vocabCns.length, createPartitioner, tmpPath, hadoopConfig, true, true)
  }

  /**
    * Constructs a distributed Word2Vec matrix
    *
    * This method for constructing the matrix has to save the vocabulary counts to a temporary HDFS file.
    * To efficiently construct a Word2Vec matrix in a standalone glint cluster in the same Spark application use
    * [[glint.Client.runWithWord2VecMatrixOnSpark(sc:org\.apache\.spark\.SparkContext)* runWithWord2VecMatrixOnSpark]]
    *
    * @param args The [[glint.Word2VecArguments Word2VecArguments]]
    * @param vocabCns The array of all word counts
    * @param hadoopConfig The Hadoop configuration to use for saving vocabCns to HDFS
    * @param numParameterServers The number of parameter servers to which the matrix should be distributed
    * @return The constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def word2vecMatrix(args: Word2VecArguments,
                     vocabCns: Array[Int],
                     hadoopConfig: Configuration,
                     numParameterServers: Int): BigWord2VecMatrix = {

    val tmpPath = hdfs.saveTmpWord2VecMatrixMetadata(hadoopConfig, Word2VecMatrixMetadata(vocabCns, args, true))
    val createPartitioner =
      (partitions: Int, keys: Long) => RangePartitioner(numParameterServers, keys, PartitionBy.COL)
    word2vecMatrix(args, vocabCns.length, createPartitioner, tmpPath, hadoopConfig, true, true)
  }

  /**
    * Loads a distributed Word2Vec matrix
    *
    * @param hdfsPath The HDFS base path from which the matrix' initial data should be loaded from
    * @param hadoopConfig The Hadoop configuration to use for loading the initial data from HDFS
    * @param trainable Whether the loaded matrix should be retrainable, requiring more data being loaded
    * @return The constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def loadWord2vecMatrix(hdfsPath: String,
                         hadoopConfig: Configuration,
                         trainable: Boolean = false): BigWord2VecMatrix = {

    val m = hdfs.loadWord2VecMatrixMetadata(hdfsPath, hadoopConfig)
    if (trainable && !m.trainable) {
      throw new ModelCreationException("Cannot create trainable model from untrainable saved data")
    }
    val numParameterServers = hdfs.countPartitionData(hdfsPath, hadoopConfig, pathPostfix = "/glint/data/u/")
    val createPartitioner =
      (partitions: Int, keys: Long) => RangePartitioner(numParameterServers, keys, PartitionBy.COL)
    word2vecMatrix(m.args, m.vocabCns.length, createPartitioner, hdfsPath, hadoopConfig, false, trainable)
  }

  private def fmpairMatrix(args: FMPairArguments,
                           numFeatures: Int,
                           avgActiveFeatures: Int,
                           createPartitioner: (Int, Long) => Partitioner,
                           hdfsPath: Option[String],
                           hadoopConfig: Option[Configuration],
                           trainable: Boolean): BigFMPairMatrix = {

    val serHadoopConfig = hadoopConfig.map(c => new SerializableHadoopConfiguration(c))

    val propFunction = (partition: Partition) => Props(classOf[PartialMatrixFMPair], partition, AggregateAdd(),
      hdfsPath, serHadoopConfig, args, numFeatures, avgActiveFeatures, trainable)

    val objFunction = (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
      new AsyncBigFMPairMatrix(partitioner, models, config, AggregateAdd(), numFeatures, args.k, trainable)

    create[BigFMPairMatrix](args.k, 1, createPartitioner, propFunction, objFunction)
  }

  /**
   * Construct a distributed FM-Pair matrix
   *
   * @param args The [[glint.FMPairArguments FMPairArguments]]
   * @param numFeatures The number of features
   * @param avgActiveFeatures The average number of active features. Not an important parameter but used for
   *                          determining good array pool sizes against garbage collection.
   * @return The constructed [[glint.models.client.BigFMPairMatrix BigFMPairMatrix]]
   */
  def fmpairMatrix(args: FMPairArguments, numFeatures: Int, avgActiveFeatures: Int = 2): BigFMPairMatrix = {
    val createPartitioner = (partitions: Int, keys: Long) => RangePartitioner(partitions, keys, PartitionBy.COL)
    fmpairMatrix(args, numFeatures, avgActiveFeatures, createPartitioner, None, None, true)
  }

  /**
    * Construct a distributed FM-Pair matrix
    *
    * @param args The [[glint.FMPairArguments FMPairArguments]]
    * @param numFeatures The number of features
    * @param avgActiveFeatures The average number of active features. Not an important parameter but used for
    *                          determining good array pool sizes against garbage collection.
    * @param numParameterServers The number of parameter servers to which the matrix should be distributed
    * @return The constructed [[glint.models.client.BigFMPairMatrix BigFMPairMatrix]]
    */
  def fmpairMatrix(args: FMPairArguments,
                   numFeatures: Int,
                   avgActiveFeatures: Int,
                   numParameterServers: Int): BigFMPairMatrix = {

    val createPartitioner =
      (partitions: Int, keys: Long) => RangePartitioner(numParameterServers, keys, PartitionBy.COL)
    fmpairMatrix(args, numFeatures, avgActiveFeatures, createPartitioner, None, None, true)
  }

  /**
    * Loads a distributed FM-Pair matrix
    *
    * @param hdfsPath The HDFS base path from which the matrix' initial data should be loaded from
    * @param hadoopConfig The Hadoop configuration to use for loading the initial data from HDFS
    * @param trainable Whether the loaded matrix should be retrainable, requiring more data being loaded
    * @return The constructed [[glint.models.client.BigFMPairMatrix BigFMPairMatrix]]
    */
  def loadFMPairMatrix(hdfsPath: String, hadoopConfig: Configuration, trainable: Boolean = false): BigFMPairMatrix = {

    val m = hdfs.loadFMPairMetadata(hdfsPath, hadoopConfig)
    if (trainable && !m.trainable) {
      throw new ModelCreationException("Cannot create trainable model from untrainable saved data")
    }
    val numParameterServers = hdfs.countPartitionData(hdfsPath, hadoopConfig, pathPostfix = "/glint/data/v/")
    val createPartitioner =
      (partitions: Int, keys: Long) => RangePartitioner(numParameterServers, keys, PartitionBy.COL)
    fmpairMatrix(m.args, m.numFeatures, m.avgActiveFeatures, createPartitioner, Some(hdfsPath), Some(hadoopConfig),
      trainable)
  }

  private def vector[V: Numerical : TypeTag](keys: Long,
                                             modelsPerServer: Int,
                                             createPartitioner: (Int, Long) => Partitioner,
                                             hdfsPath: Option[String],
                                             hadoopConfig: Option[Configuration],
                                             reverseServers: Boolean): BigVector[V] = {

    val serHadoopConfig = hadoopConfig.map(new SerializableHadoopConfiguration(_))

    val propFunction = Numerical.asString[V] match {
      case "Int" => (partition: Partition) => Props(classOf[PartialVectorInt], partition, hdfsPath, serHadoopConfig)
      case "Long" => (partition: Partition) => Props(classOf[PartialVectorLong], partition, hdfsPath, serHadoopConfig)
      case "Float" => (partition: Partition) => Props(classOf[PartialVectorFloat], partition, hdfsPath, serHadoopConfig)
      case "Double" => (partition: Partition) => Props(classOf[PartialVectorDouble], partition, hdfsPath, serHadoopConfig)
      case x => throw new ModelCreationException(s"Cannot create model for unsupported value type $x")
    }

    val objFunction = Numerical.asString[V] match {
      case "Int" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigVectorInt(partitioner, models, config, keys).asInstanceOf[BigVector[V]]
      case "Long" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigVectorLong(partitioner, models, config, keys).asInstanceOf[BigVector[V]]
      case "Float" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigVectorFloat(partitioner, models, config, keys).asInstanceOf[BigVector[V]]
      case "Double" => (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
        new AsyncBigVectorDouble(partitioner, models, config, keys).asInstanceOf[BigVector[V]]
      case x => throw new ModelCreationException(s"Cannot create model for unsupported value type $x")
    }

    create[BigVector[V]](keys, modelsPerServer, createPartitioner, propFunction, objFunction, reverseServers)
  }

  /**
    * Constructs a distributed vector (indexed by key: Long) for specified type of values
    *
    * @param keys The number of rows
    * @param modelsPerServer The number of partial models to store per parameter server (default: 1)
    * @tparam V The type of values to store, must be one of the following: Int, Long, Double or Float
    * @return The constructed [[glint.models.client.BigVector BigVector]]
    */
  def vector[V: Numerical : TypeTag](keys: Long, modelsPerServer: Int = 1): BigVector[V] = {
    vector(keys, modelsPerServer, (partitions: Int, keys: Long) => RangePartitioner(partitions, keys))
  }

  /**
    * Constructs a distributed vector (indexed by key: Long) for specified type of values
    *
    * @param keys The number of keys
    * @param modelsPerServer The number of partial models to store per parameter server
    * @param createPartitioner A function that creates a [[glint.partitioning.Partitioner partitioner]] that partitions
    *                          keys
    * @tparam V The type of values to store, must be one of the following: Int, Long, Double or Float
    * @return The constructed [[glint.models.client.BigVector BigVector]]
    */
  def vector[V: Numerical : TypeTag](keys: Long,
                                     modelsPerServer: Int,
                                     createPartitioner: (Int, Long) => Partitioner): BigVector[V] = {
    vector(keys, modelsPerServer, createPartitioner, None, None, false)
  }

  /**
   * Loads a saved distributed vector for specified type of values.
   * Keep in mind that there will be no error thrown when specifying a wrong type
   * but the loaded vector will not work as intended.
   *
   * @param hdfsPath The HDFS base path from which the vectors initial data should be loaded from
   * @param hadoopConfig The Hadoop configuration to use for loading the initial data from HDFS
   * @tparam V The type of values to store, must be one of the following: Int, Long, Double or Float
   * @return The constructed [[glint.models.client.BigVector BigVector]]
   */
  def loadVector[V: Numerical : TypeTag](hdfsPath: String, hadoopConfig: Configuration): BigVector[V] = {
    val m = hdfs.loadVectorMetadata(hdfsPath, hadoopConfig)
    vector(m.size, 1, m.createPartitioner, Some(hdfsPath), Some(hadoopConfig), false)
  }

  private def fmpairVector(args: FMPairArguments,
                           numFeatures: Int,
                           avgActiveFeatures: Int,
                           createPartitioner: (Int, Long) => Partitioner,
                           hdfsPath: Option[String],
                           hadoopConfig: Option[Configuration],
                           trainable: Boolean): BigFMPairVector = {

    val serHadoopConfig = hadoopConfig.map(c => new SerializableHadoopConfiguration(c))

    val propFunction = (partition: Partition) => Props(classOf[PartialVectorFMPair], partition, hdfsPath,
      serHadoopConfig, args, numFeatures, avgActiveFeatures, trainable)

    val objFunction = (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
      new AsyncBigFMPairVector(partitioner, models, config, numFeatures, trainable)

    create[BigFMPairVector](numFeatures, 1, createPartitioner, propFunction, objFunction, true)
  }

  /**
   * Construct a distributed FM-Pair vector
   *
   * @param args The [[glint.FMPairArguments FMPairArguments]]
   * @param numFeatures The number of features
   * @param avgActiveFeatures The average number of active features. Not an important parameter but used for
   *                          determining good array pool sizes against garbage collection.
   * @return The constructed [[glint.models.client.BigFMPairVector BigFMPairVector]]
   */
  def fmpairVector(args: FMPairArguments, numFeatures: Int, avgActiveFeatures: Int = 2): BigFMPairVector = {
    val createPartitioner = (partitions: Int, keys: Long) => RangePartitioner(partitions, keys)
    fmpairVector(args, numFeatures, avgActiveFeatures, createPartitioner, None, None, true)
  }

  /**
   * Construct a distributed FM-Pair vector
   *
   * @param args The [[glint.FMPairArguments FMPairArguments]]
   * @param numFeatures The number of features
   * @param avgActiveFeatures The average number of active features. Not an important parameter but used for
   *                          determining good array pool sizes against garbage collection.
   * @param numParameterServers The number of parameter servers to which the vector should be distributed
   * @return The constructed [[glint.models.client.BigFMPairVector BigFMPairVector]]
   */
  def fmpairVector(args: FMPairArguments,
                   numFeatures: Int,
                   avgActiveFeatures: Int,
                   numParameterServers: Int): BigFMPairVector = {

    val createPartitioner =
      (partitions: Int, keys: Long) => RangePartitioner(numParameterServers, keys)
    fmpairVector(args, numFeatures, avgActiveFeatures, createPartitioner, None, None, true)
  }

  /**
   * Loads a distributed FM-Pair vector
   *
   * @param hdfsPath The HDFS base path from which the vectors initial data should be loaded from
   * @param hadoopConfig The Hadoop configuration to use for loading the initial data from HDFS
   * @param trainable Whether the loaded matrix should be retrainable, requiring more data being loaded
   * @return The constructed [[glint.models.client.BigFMPairVector BigFMPairVector]]
   */
  def loadFMPairVector(hdfsPath: String, hadoopConfig: Configuration, trainable: Boolean = false): BigFMPairVector = {

    val m = hdfs.loadFMPairMetadata(hdfsPath, hadoopConfig)
    if (trainable && !m.trainable) {
      throw new ModelCreationException("Cannot create trainable model from untrainable saved data")
    }
    val numParameterServers = hdfs.countPartitionData(hdfsPath, hadoopConfig, pathPostfix = "/glint/data/w/")
    val createPartitioner =
      (partitions: Int, keys: Long) => RangePartitioner(numParameterServers, keys)
    fmpairVector(m.args, m.numFeatures, m.avgActiveFeatures, createPartitioner, Some(hdfsPath), Some(hadoopConfig),
      trainable)
  }

  /**
    * @return A future containing an array of available servers
    */
  def serverList(): Future[Array[ActorRef]] = {
    (master ? new ServerList()).mapTo[Array[ActorRef]]
  }

  /**
    * @return A future containing an array of connected clients
    */
  private def clientList(): Future[Array[ActorRef]] = {
    (master ? new ClientList()).mapTo[Array[ActorRef]]
  }

  /**
    * Stops the glint client
    */
  def stop(): Unit = {
    system.terminate()
  }

  /**
    * Terminates a standalone glint cluster integrated in this or another Spark application.
    * Also stops the glint client itself
    *
    * @param sc The spark context
    * @param terminateOtherClients If other clients should be terminated. This is the necessary if a glint cluster in
    *                              another Spark application should be terminated.
    */
  def terminateOnSpark(sc: SparkContext, terminateOtherClients: Boolean = false): Unit = {
    if (terminateOtherClients) {
      Await.result(clientList(), timeout.duration).foreach { c =>
        if (c.actorRef != actor) {
          c ! PoisonPill
        }
      }
    }

    val shutdownTimeout = config.getDuration("glint.default.shutdown-timeout", TimeUnit.MILLISECONDS)
    Client.terminateOnSpark(sc, shutdownTimeout milliseconds)
    stop()
  }
}

/**
  * Contains functions to easily create a client object that is connected to the glint cluster.
  *
  * You can construct a client with a specific configuration:
  * {{{
  *   import glint.Client
  *
  *   import java.io.File
  *   import com.typesafe.config.ConfigFactory
  *
  *   val config = ConfigFactory.parseFile(new File("/your/file.conf"))
  *   val client = Client(config)
  * }}}
  *
  * The resulting client object can then be used to create distributed matrices or vectors on the available parameter
  * servers:
  * {{{
  *   val matrix = client.matrix[Double](10000, 50)
  * }}}
  */
object Client {

  /**
    * Constructs a client with the default configuration
    *
    * @return The client
    */
  def apply(): Client = {
    this(ConfigFactory.empty())
  }

  /**
    * Constructs a client
    *
    * @param host The master host name
    * @return A future Client
    */
  def apply(host: String): Client = {
    apply(getHostConfig(host))
  }

  /**
    * Constructs a client
    *
    * @param config The configuration
    * @return A future Client
    */
  def apply(config: Config): Client = {
    val conf = getConfigDefaultFallback(config)
    Await.result(start(conf), conf.getDuration("glint.client.timeout", TimeUnit.MILLISECONDS) milliseconds)
  }

  /**
    * Implementation to start a client by constructing an ActorSystem and establishing a connection to a master. It
    * creates the Client object and checks if its registration actually succeeds
    *
    * @param config The configuration
    * @return The future client
    */
  private def start(config: Config): Future[Client] = {

    // Get information from config
    val masterHost = config.getString("glint.master.host")
    val masterPort = config.getInt("glint.master.port")
    val masterName = config.getString("glint.master.name")
    val masterSystem = config.getString("glint.master.system")

    // Construct system and reference to master
    val system = ActorSystem(config.getString("glint.client.system"), config.getConfig("glint.client"))
    val master = system.actorSelection(s"akka://${masterSystem}@${masterHost}:${masterPort}/user/${masterName}")

    // Set up implicit values for concurrency
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = Timeout(config.getDuration("glint.client.timeout", TimeUnit.MILLISECONDS) milliseconds)

    // Resolve master node asynchronously
    val masterFuture = master.resolveOne()

    // Construct client based on resolved master asynchronously
    masterFuture.flatMap {
      case m =>
        val client = new Client(config, system, m)
        client.registration.map {
          case true => client
          case _ => throw new RuntimeException("Invalid client registration response from master")
        }
    }
  }


  /**
    * Starts a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @param parameterServerCores The number of cores per parameter server
    * @return A future Glint client
    */
  def runOnSpark(sc: SparkContext)
                (numParameterServers: Int = getNumExecutors(sc),
                 parameterServerCores: Int = getExecutorCores(sc)): Client = {
    runOnSpark(sc, "", numParameterServers, parameterServerCores)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    * @param host The master host name
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @param parameterServerCores The number of cores per parameter server
    * @return A future Glint client
    */
  def runOnSpark(sc: SparkContext, host: String, numParameterServers: Int, parameterServerCores: Int): Client = {
    val config = getHostConfig(host)
    runOnSpark(sc, config, numParameterServers, parameterServerCores)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    * @param config The configuration
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @param parameterServerCores The number of cores per parameter server
    * @return A future Glint client
    */
  def runOnSpark(sc: SparkContext, config: Config, numParameterServers: Int, parameterServerCores: Int): Client = {
    val conf = getConfigDefaultFallback(config)
    val (client, _) = runOnSpark(sc, conf, numParameterServers, parameterServerCores, None, None)
    client
  }

  /**
    * Starts a standalone glint cluster integrated in Spark and initialize a Word2Vec matrix on it.
    *
    * These two actions are performed together to efficiently initialize the partial Word2Vec matrices.
    * This uses the vocabulary counts broadcasted by Spark as Akka props for each local actor
    * and avoids having to save them temporarily to HDFS.
    *
    * @param sc The spark context
    * @param args The [[glint.Word2VecArguments Word2VecArguments]]
    * @param bcVocabCns The array of all word counts, broadcasted by Spark beforehand
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @param parameterServerCores The number of cores per parameter server
    * @return A future Glint client and the constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def runWithWord2VecMatrixOnSpark(sc: SparkContext)
                                  (args: Word2VecArguments,
                                   bcVocabCns: Broadcast[Array[Int]],
                                   numParameterServers: Int = getNumExecutors(sc),
                                   parameterServerCores: Int = getExecutorCores(sc)): (Client, BigWord2VecMatrix) = {
    runWithWord2VecMatrixOnSpark(sc, "", args, bcVocabCns, numParameterServers, parameterServerCores)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark and initialize a Word2Vec matrix on it.
    *
    * These two actions are performed together to efficiently initialize the partial Word2Vec matrices.
    * This uses the vocabulary counts broadcasted by Spark as Akka props for each local actor
    * and avoids having to save them temporarily to HDFS.
    *
    * @param sc The spark context
    * @param host The master host name
    * @param args The [[glint.Word2VecArguments Word2VecArguments]]
    * @param bcVocabCns The array of all word counts, broadcasted by Spark beforehand
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @param parameterServerCores The number of cores per parameter server
    * @return A future Glint client and the constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def runWithWord2VecMatrixOnSpark(sc: SparkContext,
                                   host: String,
                                   args: Word2VecArguments,
                                   bcVocabCns: Broadcast[Array[Int]],
                                   numParameterServers: Int,
                                   parameterServerCores: Int): (Client, BigWord2VecMatrix) = {
    val config = getHostConfig(host)
    runWithWord2VecMatrixOnSpark(sc, config, args, bcVocabCns, numParameterServers, parameterServerCores)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark and initialize a Word2Vec matrix on it.
    *
    * These two actions are performed together to efficiently initialize the partial Word2Vec matrices.
    * This uses the vocabulary counts broadcasted by Spark as Akka props for each local actor
    * and avoids having to save them temporarily to HDFS.
    *
    * @param sc The spark context
    * @param config The configuration
    * @param args The [[glint.Word2VecArguments Word2VecArguments]]
    * @param bcVocabCns The array of all word counts, broadcasted by Spark beforehand
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @param parameterServerCores The number of cores per parameter server
    * @return A future Glint client and and the constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def runWithWord2VecMatrixOnSpark(sc: SparkContext,
                                   config: Config,
                                   args: Word2VecArguments,
                                   bcVocabCns: Broadcast[Array[Int]],
                                   numParameterServers: Int,
                                   parameterServerCores: Int): (Client, BigWord2VecMatrix) = {

    val configDefaultFallback = getConfigDefaultFallback(config)
    val (client, Some(model)) = runOnSpark(
      sc, configDefaultFallback, numParameterServers, parameterServerCores, Some(args), Some(bcVocabCns))
    (client, model)
  }

  private def runOnSpark(sc: SparkContext,
                         config: Config,
                         numParameterServers: Int,
                         parameterServerCores: Int,
                         args: Option[Word2VecArguments],
                         bcVocabCnsOpt: Option[Broadcast[Array[Int]]]): (Client, Option[BigWord2VecMatrix]) = {
    @transient
    implicit val ec = ExecutionContext.Implicits.global

    val clientTimeout = config.getDuration("glint.client.timeout", TimeUnit.MILLISECONDS) milliseconds
    val shutdownTimeout = config.getDuration("glint.default.shutdown-timeout", TimeUnit.MILLISECONDS) milliseconds

    // Defined upfront for easier error handling
    var masterSystem: Option[ActorSystem] = None
    var client: Option[Client] = None
    var partitionMasterSystem: Option[ActorSystem] = None

    try {
      // Start master
      val (s, _) = Await.result(Master.run(config), clientTimeout)
      masterSystem = Some(s)
      sys.addShutdownHook {
        terminateAndWait(masterSystem.get, config)
      }

      // Construct client
      client = Some(Client(config))

      // Start partition master
      val numKeys = args.map(_.vectorSize).getOrElse(numParameterServers)
      val partitioner = RangePartitioner(numParameterServers, numKeys, PartitionBy.COL)
      val (ps, _) = Await.result(PartitionMaster.run(config, partitioner.all()), clientTimeout)
      partitionMasterSystem = Some(ps)

      // Start parameter servers on workers
      // If the vocabulary counts were broadcasted start them with partial models and construct a big model client
      val bigModel = if (args.isDefined && bcVocabCnsOpt.isDefined) {
        Some(word2vecMatrixServersOnSpark(
          sc, config, clientTimeout, parameterServerCores, client.get, partitioner, args.get, bcVocabCnsOpt.get))
      } else {
        serversOnSpark(sc, config, clientTimeout, parameterServerCores)
        None
      }

      // Check if the requested number of parameter servers were started
      val numStarted = Await.result(client.get.serverList().map(_.length), clientTimeout)
      if (numStarted != numParameterServers) {
        throw new ServerCreationException(s"Could not start the requested number of parameter servers. " +
          s"Requested $numParameterServers, started $numStarted")
      }

      StartedActorSystems.add(masterSystem.get)

      (client.get, bigModel)
    } catch {
      case ex: Throwable =>
        masterSystem.foreach(_.terminate())
        client.foreach(_.terminateOnSpark(sc))
        throw ex
    } finally {
      // Shutdown partition master
      partitionMasterSystem.foreach(terminateAndWait(_, config))
    }
  }

  private def serversOnSpark(sc: SparkContext, config: Config, clientTimeout: Duration, serverCores: Int): Unit = {
    val nrOfPartitions = getNumExecutors(sc) * getExecutorCores(sc)
    sc.range(0, nrOfPartitions, numSlices = nrOfPartitions).foreachPartition {
      case _ => Await.result(Server.runOnce(config, serverCores), clientTimeout)
    }
  }

  private def word2vecMatrixServersOnSpark(sc: SparkContext,
                                           config: Config,
                                           clientTimeout: Duration,
                                           serverCores: Int,
                                           client: Client,
                                           partitioner: Partitioner,
                                           args: Word2VecArguments,
                                           bcVocabCns: Broadcast[Array[Int]]): BigWord2VecMatrix = {

    // Start parameter servers and create partial models on workers
    // Return list of serialized partial model actor references
    val nrOfPartitions = getNumExecutors(sc) * getExecutorCores(sc)
    val serHadoopConfig = new SerializableHadoopConfiguration(sc.hadoopConfiguration)
    val models = sc.range(0, nrOfPartitions, numSlices = nrOfPartitions).mapPartitions { _ =>
      @transient
      implicit val ec = ExecutionContext.Implicits.global

      val partialModelFuture = Server.runOnce(config, serverCores).map {
        case Some((serverSystem, serverRef, partition)) =>
          val props = Props(classOf[PartialMatrixWord2Vec], partition, AggregateAdd(), None, Some(serHadoopConfig),
            args, bcVocabCns.value.length, Some(bcVocabCns.value), false, true)
          val actorRef = serverSystem.actorOf(props.withDeploy(Deploy.local))
          Some((Serialization.serializedActorPath(actorRef), partition.index))
        case None => None
      }
      Await.result(partialModelFuture, clientTimeout).map(x => Iterator(x)).getOrElse(Iterator())
    }.collect().sortBy(_._2).map(_._1)

    // Deserialize partial model actor references
    val extendedActorSystem = SerializationExtension(client.system).system
    val modelActorRefs = models.map(model => extendedActorSystem.provider.resolveActorRef(model))

    // Construct big model client object
    new AsyncBigWord2VecMatrix(partitioner, modelActorRefs, config, AggregateAdd(),
      bcVocabCns.value.length, args.vectorSize, args.n, true)
  }

  /**
    * Terminates a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    */
  def terminateOnSpark(sc: SparkContext, shutdownTimeout: Duration): Unit = {
    if (!sc.isStopped) {
      val nrOfPartitions = getNumExecutors(sc) * getExecutorCores(sc)
      sc.range(0, nrOfPartitions, numSlices = nrOfPartitions).foreachPartition { case _ =>
        @transient
        implicit val ec = ExecutionContext.Implicits.global
        StartedActorSystems.terminateAndWait(shutdownTimeout)
      }
    }

    @transient
    implicit val ec = ExecutionContext.Implicits.global
    StartedActorSystems.terminateAndWait(shutdownTimeout)
  }

  /**
    * Gets the number of (worker) executors in a way which is portable across yarn, standalone and local mode
    *
    * @param sc The spark context
    * @return The number of executors
    */
  def getNumExecutors(sc: SparkContext): Int = {
    // if --num-executors is set for yarn
    sc.getConf.getOption("spark.executor.instances").map(_.toInt)
      // if --total-executor-cores is set for standalone
      .orElse(sc.getConf.getOption("spark.cores.max").map(_.toInt / getExecutorCores(sc)))
      // if no config is set
      .getOrElse {

      var nrOfExecutors = sc.getExecutorMemoryStatus.size
      if (nrOfExecutors == 1) {
        // For a short period at startup we get only 1 executor, so to be safe we wait a bit
        // See https://stackoverflow.com/a/52516704
        Thread.sleep(5000)
        nrOfExecutors = sc.getExecutorMemoryStatus.size
      }
      // Subtract driver executor if not in local mode
      Math.max(nrOfExecutors - 1, 1)
    }
  }

  /**
    * Gets the number of cores per executors.
    * The number is divided by the number of cpus per task
    *
    * @param sc The spark context
    * @return The number of cores per executor divided by the number of cpus per task
    */
  def getExecutorCores(sc: SparkContext): Int = {
    val cores = sc.getConf.get("spark.executor.cores", "1").toDouble
    val taskCpus = sc.getConf.get("spark.task.cpus", "1").toDouble
    (cores / taskCpus).toInt
  }

  /**
    * Gets a configuration with the given host or the local host as master and partition master host
    *
    * @param host The host for the master and partition master or the empty string
    * @return The configuration with the given host or the local host
    */
  def getHostConfig(host: String = ""): Config = {
    val hostConfigValue = if (host.isEmpty) {
      ConfigValueFactory.fromAnyRef(InetAddress.getLocalHost.getHostAddress)
    } else {
      ConfigValueFactory.fromAnyRef(host)
    }
    ConfigFactory.empty()
      .withValue("glint.master.host", hostConfigValue)
      .withValue("glint.master.akka.remote.artery.canonical.hostname", hostConfigValue)
      .withValue("glint.partition-master.host", hostConfigValue)
      .withValue("glint.partition-master.akka.remote.artery.canonical.hostname", hostConfigValue)
  }

  /**
    * Gets the given configuration with the local host and default configuration as fallback
    *
    * @param config The configuration
    * @return The configuration with fallback
    */
  def getConfigDefaultFallback(config: Config): Config = {
    config
      .withFallback(getHostConfig())
      .withFallback(ConfigFactory.parseResourcesAnySyntax("glint"))
      .resolve()
  }
}


/**
  * Word2Vec arguments
  *
  * @param vectorSize The (full) vector size
  * @param window The window size
  * @param batchSize The minibatch size
  * @param n The number of negative examples to create per output word
  * @param unigramTableSize The size of the unigram table for efficient generation of random negative words.
  *                         Smaller sizes can prevent OutOfMemoryError but might lead to worse results
  */
case class Word2VecArguments(vectorSize: Int,
                             window: Int,
                             batchSize: Int,
                             n: Int,
                             unigramTableSize: Int = 100000000)


/**
  * FM-Pair arguments
  *
  * @param k The number of dimensions / latent factors
  * @param batchSize The minibatch size
  * @param lr The Adagrad learning rate
  * @param linearReg The regularization rate for the linear weights
  * @param factorsReg The regularization rate for the latent factors
  */
case class FMPairArguments(k: Int = 50,
                           batchSize: Int = 4096,
                           lr: Float = 0.1f,
                           linearReg: Float = 0.6f,
                           factorsReg: Float = 0.006f)


/**
  * The client actor class. The master keeps a death watch on this actor and knows when it is terminated.
  *
  * This actor either gets terminated when the system shuts down (e.g. when the Client object is destroyed) or when it
  * crashes unexpectedly.
  */
private class ClientActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case x => log.info(s"Client actor received message ${x}")
  }

  override def postStop(): Unit = {
    context.system.terminate()
  }
}
