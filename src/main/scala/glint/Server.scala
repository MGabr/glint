package glint

import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import glint.messages.master.RegisterServer
import glint.messages.partitionmaster.{AcquirePartition, ReleasePartition}
import glint.partitioning.Partition
import glint.util.terminateAndWait

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * A parameter server
  */
private[glint] class Server extends Actor with ActorLogging {

  override def receive: Receive = {
    case x =>
      log.warning(s"Received unknown message of type ${x.getClass}")
  }
}

/**
  * The parameter server object
  */
private[glint] object Server extends StrictLogging {

  private val lock = new Semaphore(1)

  /**
    * Starts a parameter server ready to receive commands
    *
    * @param config The configuration
    * @param cores The number of cpu cores available for this parameter server
    * @return A future containing the started actor system and reference to the server actor
    */
  def run(config: Config, cores: Int = Runtime.getRuntime.availableProcessors()): Future[(ActorSystem, ActorRef)] = {
    implicit val ec = ExecutionContext.Implicits.global
    val system = startActorSystem(config, cores)
    try {
      run(config, system).map(server => (system, server))
    } catch {
      case ex: Throwable =>
        system.terminate()
        throw ex
    }
  }

  private def startActorSystem(config: Config, cores: Int): ActorSystem = {
    logger.debug(s"Starting actor system ${config.getString("glint.server.system")}")
    val name = config.getString("glint.server.system")
    val serverConfig = config.getConfig("glint.server")
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(cores))
    val system = ActorSystem(name, config = Some(serverConfig), defaultExecutionContext = Some(ec))
    system.registerOnTermination(new Runnable {
      override def run(): Unit = ec.shutdown()
    })
    system
  }

  private def run(config: Config, system: ActorSystem)(implicit ec: ExecutionContext): Future[ActorRef] = {
    logger.debug("Starting server actor")
    val server = system.actorOf(Props[Server], config.getString("glint.server.name"))

    logger.debug("Reading master information from config")
    val masterHost = config.getString("glint.master.host")
    val masterPort = config.getInt("glint.master.port")
    val masterName = config.getString("glint.master.name")
    val masterSystem = config.getString("glint.master.system")

    logger.info(s"Registering with master ${masterSystem}@${masterHost}:${masterPort}/user/${masterName}")
    implicit val timeout = Timeout(config.getDuration("glint.server.registration-timeout", TimeUnit.MILLISECONDS) milliseconds)
    val master = system.actorSelection(s"akka://${masterSystem}@${masterHost}:${masterPort}/user/${masterName}")
    val registration = master ? RegisterServer(server)

    registration.onFailure {
      case ex =>
        logger.error(s"Shutting down actor system ${config.getString("glint.server.system")} on failure", ex)
        terminateAndWait(system, config)
    }
    registration.map {
      case a =>
        logger.info("Server successfully registered with master")
        server
    }
  }

  /**
    * Starts a parameter server once per JVM, even if this method is called multiple times
    * and only if there is still a partition available from the partition master
    *
    * @param config The configuration
    * @param cores The number of cpu cores available for this parameter server
    * @return A future containing the started actor system, a reference to the server actor and the server partition
    */
  def runOnce(config: Config, cores: Int): Future[Option[(ActorSystem, ActorRef, Partition)]] = {
    lock.acquire()
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = Timeout(config.getDuration("glint.client.timeout", TimeUnit.MILLISECONDS) milliseconds)
    val future = if (!StartedActorSystems.hasStartedServer) {
      val system = startActorSystem(config, cores)
      StartedActorSystems.add(system, isServer = true)

      val partitionMaster = getPartitionMaster(config, system)
      val partitionFuture = (partitionMaster ? AcquirePartition()).flatMap {
        case Some(partition: Partition) =>
          // we acquired a partition, so we can start a parameter server actor on this JVM
          val future = run(config, system)
          future.onFailure { case _ => partitionMaster ! ReleasePartition(partition) }
          future.map { server => Some((system, server, partition)) }
        case None =>
          // we could not acquire a partition, so we need to shut down the actor system on this JVM again
          system.terminate()
          Future.successful(None)
      }
      partitionFuture.onFailure { case _ => StartedActorSystems.remove(system, isServer = true) }
      partitionFuture
    } else {
      Future.successful(None)
    }
    lock.release()
    future
  }

  private def getPartitionMaster(config: Config, system: ActorSystem): ActorSelection = {
    logger.debug("Reading partition master information from config")
    val masterHost = config.getString("glint.partition-master.host")
    val masterPort = config.getInt("glint.partition-master.port")
    val masterName = config.getString("glint.partition-master.name")
    val masterSystem = config.getString("glint.partition-master.system")
    system.actorSelection(s"akka://${masterSystem}@${masterHost}:${masterPort}/user/${masterName}")
  }

}
