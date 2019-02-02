package glint

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Props}
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import glint.messages.partitionmaster.{AcquirePartition, ReleasePartition}
import glint.partitioning.Partition

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * A partition master which allows parameter servers to acquire available partitions.
  * Useful as distributed semaphore-like structure for distribution of parameter servers / partitions to Spark executors
  *
  * @param partitions The partitions to manage
  */
private[glint] class PartitionMaster(val partitions: mutable.Queue[Partition]) extends Actor with ActorLogging {

  override def receive: Receive = {

    case AcquirePartition() =>
      if (partitions.isEmpty) {
        log.info("No partition which could be acquired")
        sender ! None
      } else {
        val partition = partitions.dequeue()
        log.info(s"Acquired partition ${partition.index}")
        sender ! Some(partition)
      }

    case ReleasePartition(partition) =>
      partitions.enqueue(partition)
      log.info(s"Released partition ${partition.index}")

  }
}

private[glint] object PartitionMaster extends StrictLogging {

  /**
    * Starts a partition master node ready to receive commands
    *
    * @param config The configuration
    * @param partitions The available partitions to manage
    * @return A future containing the started actor system and reference to the partition master actor
    */
  def run(config: Config, partitions: Seq[Partition]): Future[(ActorSystem, ActorRef)] = {

    logger.debug("Starting partition master actor system")
    val system = ActorSystem(
      config.getString("glint.partition-master.system"),
      config.getConfig("glint.partition-master"))

    logger.debug("Starting partition master")
    val props = Props(classOf[PartitionMaster], mutable.Queue(partitions : _*))
    val master = system.actorOf(props, config.getString("glint.partition-master.name"))

    implicit val timeout = Timeout(
      config.getDuration("glint.partition-master.startup-timeout", TimeUnit.MILLISECONDS) milliseconds)
    implicit val ec = ExecutionContext.Implicits.global

    val address = Address("akka",
      config.getString("glint.partition-master.system"),
      config.getString("glint.partition-master.host"),
      config.getInt("glint.partition-master.port"))

    system.actorSelection(master.path.toSerializationFormat).resolveOne().map {
      case a: ActorRef =>
        logger.info("Partition master successfully started")
        (system, master)
    }
  }
}
