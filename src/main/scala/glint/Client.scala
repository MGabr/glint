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
import glint.messages.master.{RegisterClient, ServerList}
import glint.models.client.async._
import glint.models.client.{BigMatrix, BigVector, BigWord2VecMatrix}
import glint.models.server._
import glint.models.server.aggregate.{Aggregate, AggregateAdd}
import glint.partitioning.by.PartitionBy
import glint.partitioning.by.PartitionBy.PartitionBy
import glint.partitioning.range.RangePartitioner
import glint.partitioning.{Partition, Partitioner}
import glint.serialization.SerializableHadoopConfiguration
import glint.util._
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
    * @tparam M The final model type to generate
    * @return The generated model
    */
  private def create[M](keys: Long,
                        modelsPerServer: Int,
                        createPartitioner: (Int, Long) => Partitioner,
                        generateServerProp: Partition => Props,
                        generateClientObject: (Partitioner, Array[ActorRef], Config) => M): M = {

    // Get a list of servers
    val listOfServers = serverList()

    // Construct a big model based on the list of servers
    val bigModelFuture = listOfServers.map { servers =>

      // Check if there are servers online
      if (servers.isEmpty) {
        throw new ModelCreationException("Cannot create a model without active parameter servers")
      }

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

    matrix[V](rows, cols, modelsPerServer, aggregate, partitionBy, (partitions: Int, keys: Long) => RangePartitioner(partitions, keys, partitionBy))
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
    * Loads a saved distributed matrix (indexed by (row: Long, col: Long)) for specified type of values.
    * Keep in mind that there will be no error thrown when specifying a wrong type
    * but the loaded matrix will not work as intended.
    *
    * @param hdfsPath The HDFS base path from which the matrix' initial data should be loaded from
    * @param hadoopConfig The Hadoop configuration to use for loading the initial data from HDFS
    * @tparam V The type of values to store, must be one of the following: Int, Long, Double or Float
    * @return The constructed [[glint.models.client.BigMatrix BigMatrix]]
    */
  def loadMatrix[V: Numerical : TypeTag](hdfsPath: String, hadoopConfig: Configuration): BigMatrix[V] = {

    val m = hdfs.loadMetadata(hdfsPath, hadoopConfig)
    matrix(m.rows, m.cols, 1, m.aggregate, m.partitionBy, m.createPartitioner, Some(hdfsPath), Some(hadoopConfig))
  }

  /**
    * Constructs a distributed Word2Vec matrix
    *
    * This method for constructing the matrix has to send the vocabulary counts as serialized Akka props to remote
    * actors and is mainly intended for testing outside of spark. To efficiently construct a Word2Vec matrix use
    * [[glint.Client.runWithWord2VecMatrixOnSpark()]]
    *
    * @param vectorSize The (full) vector size
    * @param n The number of negative examples to create per output word
    * @param unigramTableSize The size of the unigram table for efficient generation of random negative words.
    *                         Smaller sizes can prevent OutOfMemoryError but might lead to worse results
    * @return The constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def word2vecMatrix(vocabCns: Array[Int],
                     vectorSize: Int,
                     n: Int,
                     unigramTableSize: Int = 100000000): BigWord2VecMatrix = {

    val createPartitioner = (partitions: Int, keys: Long) => RangePartitioner(partitions, keys, PartitionBy.COL)

    val propFunction = (partition: Partition) =>
      Props(classOf[PartialMatrixWord2Vec], partition, AggregateAdd(), None, None, vectorSize, vocabCns, n, unigramTableSize)

    val objFunction = (partitioner: Partitioner, models: Array[ActorRef], config: Config) =>
      new AsyncBigWord2VecMatrix(partitioner, models, config, AggregateAdd(), vocabCns.length, vectorSize, n)

    create[BigWord2VecMatrix](vectorSize, 1, createPartitioner, propFunction, objFunction)
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
    vector[V](keys, modelsPerServer, (partitions: Int, keys: Long) => RangePartitioner(partitions, keys))
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

    val propFunction = Numerical.asString[V] match {
      case "Int" => (partition: Partition) => Props(classOf[PartialVectorInt], partition)
      case "Long" => (partition: Partition) => Props(classOf[PartialVectorLong], partition)
      case "Float" => (partition: Partition) => Props(classOf[PartialVectorFloat], partition)
      case "Double" => (partition: Partition) => Props(classOf[PartialVectorDouble], partition)
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

    create[BigVector[V]](keys, modelsPerServer, createPartitioner, propFunction, objFunction)

  }

  /**
    * @return A future containing an array of available servers
    */
  def serverList(): Future[Array[ActorRef]] = {
    (master ? new ServerList()).mapTo[Array[ActorRef]]
  }

  /**
    * Stops the glint client
    */
  def stop(): Unit = {
    system.terminate()
  }

  /**
    * Terminates a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    */
  def terminateOnSpark(sc: SparkContext): Unit = {
    val shutdownTimeout = config.getDuration("glint.default.shutdown-timeout", TimeUnit.MILLISECONDS)
    Client.terminateOnSpark(sc, shutdownTimeout milliseconds)
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
    * @param config The configuration
    * @return A future Client
    */
  def apply(config: Config): Client = {
    val default = ConfigFactory.parseResourcesAnySyntax("glint")
    val conf = config.withFallback(default).resolve()
    Await.result(start(conf), conf.getDuration("glint.client.timeout", TimeUnit.MILLISECONDS) milliseconds)
  }


  /**
    * Starts a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @return A future Glint client
    */
  def runOnSpark(sc: SparkContext)
                (numParameterServers: Int = getNumExecutors(sc)): Client = {
    runOnSpark(sc, "", numParameterServers)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    * @param host The master host name
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @return A future Glint client
    */
  def runOnSpark(sc: SparkContext, host: String, numParameterServers: Int): Client = {
    val config = getConfig(host)
    runOnSpark(sc, config, numParameterServers)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    * @param config The configuration
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @return A future Glint client
    */
  def runOnSpark(sc: SparkContext, config: Config, numParameterServers: Int): Client = {
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
      val partitioner = RangePartitioner(numParameterServers, numParameterServers)
      val (ps, _) = Await.result(PartitionMaster.run(config, partitioner.all()), clientTimeout)
      partitionMasterSystem = Some(ps)

      // Start parameter servers on workers
      val nrOfPartitions = getNumExecutors(sc) * getExecutorCores(sc)
      sc.range(0, nrOfPartitions, numSlices = nrOfPartitions).foreachPartition {
        case _ => Await.result(Server.runOnce(config), clientTimeout)
      }

      // Check if the requested number of parameter servers were started
      val numStartedParameterServers = Await.result(client.get.serverList().map(_.length), clientTimeout)
      if (numStartedParameterServers != numParameterServers) {
        throw new ServerCreationException(
          s"Could not start the requested number of parameter servers. " +
          s"Requested $numParameterServers, started $numStartedParameterServers")
      }

      StartedActorSystems.add(masterSystem.get)
      StartedActorSystems.add(client.get.system)

      client.get
    } catch {
      case ex: Throwable =>
        masterSystem.foreach(_.terminate())
        client.foreach(_.stop())
        StartedActorSystems.terminateAndWait(shutdownTimeout)
        throw ex
    } finally {
      // Shutdown partition master
      partitionMasterSystem.foreach(terminateAndWait(_, config))
    }
  }

  /**
    * Starts a standalone glint cluster integrated in Spark and initialize a Word2Vec matrix on it.
    *
    * These two actions are performed together to efficiently initialize the partial Word2Vec matrices.
    * This uses the vocabulary counts broadcasted by Spark as Akka props for each local actor
    * and avoids having to send them as serialized Akka props to remote actors.
    *
    * @param sc The spark context
    * @param bcVocabCns The array of all word counts, broadcasted by Spark beforehand
    * @param vectorSize The (full) vector size
    * @param n The number of negative examples to create per output word
    * @param unigramTableSize The size of the unigram table for efficient generation of random negative words.
    *                         Smaller sizes can prevent OutOfMemoryError but might lead to worse results
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @return A future Glint client and the constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def runWithWord2VecMatrixOnSpark(sc: SparkContext)
                                  (bcVocabCns: Broadcast[Array[Int]],
                                   vectorSize: Int,
                                   n: Int,
                                   unigramTableSize: Int = 100000000,
                                   numParameterServers: Int = getNumExecutors(sc)): (Client, BigWord2VecMatrix) = {
    runWithWord2VecMatrixOnSpark(sc, "", bcVocabCns, vectorSize, n, unigramTableSize, numParameterServers)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark and initialize a Word2Vec matrix on it.
    *
    * These two actions are performed together to efficiently initialize the partial Word2Vec matrices.
    * This uses the vocabulary counts broadcasted by Spark as Akka props for each local actor
    * and avoids having to send them as serialized Akka props to remote actors.
    *
    * @param sc The spark context
    * @param host The master host name
    * @param bcVocabCns The array of all word counts, broadcasted by Spark beforehand
    * @param vectorSize The (full) vector size
    * @param n The number of negative examples to create per output word
    * @param unigramTableSize The size of the unigram table for efficient generation of random negative words.
    *                         Smaller sizes can prevent OutOfMemoryError but might lead to worse results
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @return A future Glint client and the constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def runWithWord2VecMatrixOnSpark(sc: SparkContext,
                                   host: String,
                                   bcVocabCns: Broadcast[Array[Int]],
                                   vectorSize: Int,
                                   n: Int,
                                   unigramTableSize: Int,
                                   numParameterServers: Int): (Client, BigWord2VecMatrix) = {
    val config = getConfig(host)
    runWithWord2VecMatrixOnSpark(sc, config, bcVocabCns, vectorSize, n, unigramTableSize, numParameterServers)
  }

  /**
    * Starts a standalone glint cluster integrated in Spark and initialize a Word2Vec matrix on it.
    *
    * These two actions are performed together to efficiently initialize the partial Word2Vec matrices.
    * This uses the vocabulary counts broadcasted by Spark as Akka props for each local actor
    * and avoids having to send them as serialized Akka props to remote actors.
    *
    * @param sc The spark context
    * @param config The configuration
    * @param bcVocabCns The array of all word counts, broadcasted by Spark beforehand
    * @param vectorSize The (full) vector size
    * @param n The number of negative examples to create per output word
    * @param unigramTableSize The size of the unigram table for efficient generation of random negative words.
    *                         Smaller sizes can prevent OutOfMemoryError but might lead to worse results
    * @param numParameterServers The number of glint parameter servers to create on the cluster.
    *                            The maximum possible number is the number of executors.
    * @return A future Glint client and and the constructed [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]]
    */
  def runWithWord2VecMatrixOnSpark(sc: SparkContext,
                                   config: Config,
                                   bcVocabCns: Broadcast[Array[Int]],
                                   vectorSize: Int,
                                   n: Int,
                                   unigramTableSize: Int,
                                   numParameterServers: Int): (Client, BigWord2VecMatrix) = {
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
      val partitioner = RangePartitioner(numParameterServers, vectorSize, PartitionBy.COL)
      val (ps, _) = Await.result(PartitionMaster.run(config, partitioner.all()), clientTimeout)
      partitionMasterSystem = Some(ps)

      // Start parameter servers and create 'numParameterServers' partial models on workers
      // Return list of serialized partial model actor references
      val nrOfPartitions = getNumExecutors(sc) * getExecutorCores(sc)
      val models = sc.range(0, nrOfPartitions, numSlices = nrOfPartitions).mapPartitions { _ =>
        @transient
        implicit val ec = ExecutionContext.Implicits.global

        val partialModelFuture = Server.runOnce(config).map {
          case Some((serverSystem, serverRef, partition)) =>
            val props = Props(classOf[PartialMatrixWord2Vec], partition, AggregateAdd(), None, None, vectorSize, bcVocabCns.value, n, unigramTableSize)
            val actorRef = serverSystem.actorOf(props.withDeploy(Deploy.local))
            Some((Serialization.serializedActorPath(actorRef), partition.index))
          case None => None
        }
        Await.result(partialModelFuture, clientTimeout).map(x => Iterator(x)).getOrElse(Iterator())
      }.collect().sortBy(_._2).map(_._1)

      // Check if the requested number of parameter servers were started
      val numStartedParameterServers = Await.result(client.get.serverList().map(_.length), clientTimeout)
      if (numStartedParameterServers != numParameterServers) {
        throw new ServerCreationException(
          s"Could not start the requested number of parameter servers. " +
            s"Requested $numParameterServers, started $numStartedParameterServers")
      }

      // deserialize partial model actor references
      val extendedActorSystem = SerializationExtension(client.get.system).system
      val modelRefs = models.map(model => extendedActorSystem.provider.resolveActorRef(model))

      // Construct a big model client object
      val bigModel = new AsyncBigWord2VecMatrix(partitioner, modelRefs, config, AggregateAdd(), bcVocabCns.value.length, vectorSize, n)

      StartedActorSystems.add(masterSystem.get)
      StartedActorSystems.add(client.get.system)

      (client.get, bigModel)
    } catch {
      case ex: Throwable =>
        masterSystem.foreach(_.terminate())
        client.foreach(_.stop())
        StartedActorSystems.terminateAndWait(shutdownTimeout)
        throw ex
    } finally {
      // Shutdown partition master
      partitionMasterSystem.foreach(terminateAndWait(_, config))
    }
  }

  /**
    * Gets the default configuration with the given host or the local host as master and partition master host
    *
    * @param host The host for the master and partition master or the empty string
    * @return The  default configuration with the given host or the local host
    */
  private def getConfig(host: String): Config = {
    val default = ConfigFactory.parseResourcesAnySyntax("glint").resolve()
    if (host.isEmpty) {
      val localhost = ConfigValueFactory.fromAnyRef(InetAddress.getLocalHost.getHostAddress)
      default
        .withValue("glint.master.host", localhost)
        .withValue("glint.master.akka.remote.artery.canonical.hostname", localhost)
        .withValue("glint.partition-master.host", localhost)
        .withValue("glint.partition-master.akka.remote.artery.canonical.hostname", localhost)
    } else {
      val hostConfigValue = ConfigValueFactory.fromAnyRef(host)
      default
        .withValue("glint.master.host", hostConfigValue)
        .withValue("glint.master.akka.remote.artery.canonical.hostname", hostConfigValue)
        .withValue("glint.partition-master.host", hostConfigValue)
        .withValue("glint.partition-master.akka.remote.artery.canonical.hostname", hostConfigValue)
    }
  }

  /**
    * Terminates a standalone glint cluster integrated in Spark
    *
    * @param sc The spark context
    */
  def terminateOnSpark(sc: SparkContext, shutdownTimeout: Duration): Unit = {
    val nrOfExecutors = getNumExecutors(sc)
    val executorCores = sc.getConf.get("spark.executor.cores", "1").toInt
    val nrOfPartitions = nrOfExecutors * executorCores
    sc.range(0, nrOfPartitions, numSlices = nrOfPartitions).foreachPartition { case _ =>
      @transient
      implicit val ec = ExecutionContext.Implicits.global
      StartedActorSystems.terminateAndWait(shutdownTimeout)
    }

    @transient
    implicit val ec = ExecutionContext.Implicits.global
    StartedActorSystems.terminateAndWait(shutdownTimeout)
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
}

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
}
