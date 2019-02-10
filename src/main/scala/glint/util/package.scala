package glint

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.SparkContext

package object util {

  /**
    * Terminates the actor system and waits for it to finish (or when it times out)
    *
    * @param system The actor system to terminate
    */
  def terminateAndWait(system: ActorSystem, config: Config)(implicit ec: ExecutionContext): Unit = {
    Await.result(system.terminate(), config.getDuration("glint.default.shutdown-timeout", TimeUnit.MILLISECONDS) milliseconds)
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
    * Gets the number of cores per executors
    *
    * @param sc The spark context
    * @return The number of cores per executor
    */
  def getExecutorCores(sc: SparkContext): Int = {
    sc.getConf.get("spark.executor.cores", "1").toInt
  }
}
