package glint

import java.io.File
import java.net.InetAddress

import glint.util._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * This is the main class that runs when you start Glint. By manually specifying additional command-line options it is
  * possible to start a master node or a parameter server.
  *
  * To start a master node:
  * {{{
  *   java -jar /path/to/compiled/Glint.jar master -c /path/to/glint.conf
  * }}}
  *
  * To start a parameter server node:
  * {{{
  *   java -jar /path/to/compiled/Glint.jar server -c /path/to/glint.conf
  * }}}
  *
  * Alternatively you can use the scripts provided in the ./sbin/ folder of the project to automatically construct a
  * master and servers over passwordless ssh.
  *
  * To start a master and parameter server nodes on Spark:
  * {{{
  *   spark-submit --num-executors num-servers --executor-cores server-cores /path/to/compiled/Glint.jar spark -c /path/to/glint.conf
  * }}}
  *
  */
object Main extends StrictLogging {

  /**
    * Main entry point of the application
    *
    * @param args The command-line arguments
    */
  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Options]("glint") {
      head("glint", "0.2")
      opt[File]('c', "config") valueName "<file>" action { (x, c) =>
        c.copy(config = Some(x))
      } text "The .conf file for glint"
      cmd("master") action { (_, c) =>
        c.copy(mode = "master")
      } text "Starts a master node."
      cmd("server") action { (_, c) =>
        c.copy(mode = "server")
      } text "Starts a server node."
      cmd("spark") action { (_, c) =>
        c.copy(mode = "spark")
      } text "Starts master and server nodes on Spark."
    }

    parser.parse(args, Options()) match {
      case Some(options) =>

        // Read configuration
        val config = options.config.map(c => ConfigFactory.parseFile(c)).getOrElse(ConfigFactory.empty())

        // Start specified mode of operation
        implicit val ec = ExecutionContext.Implicits.global
        options.mode match {
          case "server" => Server.run(config).onSuccess {
            case (system, ref) => sys.addShutdownHook {
              logger.info("Shutting down")
              terminateAndWait(system, config)
            }
          }
          case "master" => Master.run(config).onSuccess {
            case (system, ref) => sys.addShutdownHook {
              logger.info("Shutting down")
              terminateAndWait(system, config)
            }
          }
          case "spark" => runOnSpark(config)
          case _ =>
            parser.showUsageAsError
            System.exit(1)
        }
      case None => System.exit(1)
    }
  }

  /**
    * Command-line options
    *
    * @param mode The mode of operation (either "master" or "server")
    * @param config The configuration file to load
    */
  private case class Options(mode: String = "", config: Option[File] = None)

  /**
    * Runs master and server nodes on Spark.
    * This Spark application will run infinitely until it is killed or a client terminates it
    *
    * @param config The configuration to use
    */
  private def runOnSpark(config: Config): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Glint parameter servers"))
    val client = Client.runOnSpark(sc, config, Client.getNumExecutors(sc), Client.getExecutorCores(sc))

    val driverHost = InetAddress.getLocalHost.getHostAddress
    logger.info(s"Started Glint parameter servers as Spark application ${sc.applicationId} with master on $driverHost")

    // termination in case application is killed
    sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        client.terminateOnSpark(sc)
      }
    })

    // termination in case client system is terminated by other client
    Await.ready(client.system.whenTerminated, Duration.Inf)
    client.terminateOnSpark(sc)
    sc.stop()
  }
}
