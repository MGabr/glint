package glint

import akka.actor.{ActorSystem, Terminated}
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Object to manage actor systems started in this JVM to facilitate easy termination
  */
private[glint] object StartedActorSystems {

  @transient
  private var systems: Seq[ActorSystem] = Seq()

  @transient
  private var as: Option[ActorSystem] = None

  @transient
  private var startedServer: Boolean = false

  /**
    * Terminates all actor systems started in this JVM and waits for them to finish (or when they time out)
    *
    * @param shutdownTimeout The timeout
    * @param ec The execution context
    */
  def terminateAndWait(shutdownTimeout: Duration)(implicit ec: ExecutionContext): Unit =  {
    var future: Option[Future[Seq[Terminated]]] = None
    synchronized {
      if (systems.nonEmpty) {
        future = Some(Future.sequence(systems.map(_.terminate())))
        systems = Seq()
        startedServer = false
      }
      if (as.isDefined) {
        as = None
      }
    }
    future.foreach(Await.ready(_, shutdownTimeout))
  }

  /**
    * Gets an active actor system or creates one with given config if it does not yet exist.
    * This actor system should only be managed by this object and not be terminated manually
    *
    * @param config The configuration
    * @return The actor system
    */
  def getActorSystem(config: Config): ActorSystem = synchronized {
    if (as.isEmpty) {
      as = Some(ActorSystem("GlintClient", config))
      add(as.get)
    }
    as.get
  }

  /**
    * Adds a started actor system
    *
    * @param system The actor system
    * @param isServer Whether this actor system is a server or not
    */
  def add(system: ActorSystem, isServer: Boolean = false): Unit = synchronized {
    systems = systems :+ system
    if (isServer) {
      this.startedServer = true
    }
  }

  /**
    * Removes a actor system which was shut down again
    *
    * @param system The actor system
    * @param isServer Whether this actor system is a server or not
    */
  def remove(system: ActorSystem, isServer: Boolean = false): Unit = synchronized {
    systems = systems.filterNot(s => s == system)
    if (isServer) {
      this.startedServer = false
    }
  }

  /**
    * Returns whether a server was started on this JVM or not
    *
    * @return Whether a server was started on this JVM or not
    */
  def hasStartedServer: Boolean = this.startedServer
}
