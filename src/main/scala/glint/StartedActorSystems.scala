package glint

import akka.actor.{ActorSystem, Terminated}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Object holding all actor systems started in this JVM to facilitate easy termination
  */
private[glint] object StartedActorSystems {

  @transient
  private var systems: Seq[ActorSystem] = Seq()

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
      }
    }
    future.foreach(Await.ready(_, shutdownTimeout))
  }

  /**
    * Adds a started actor system
    *
    * @param system The actor system
    */
  def add(system: ActorSystem): Unit = synchronized {
    systems = systems :+ system
  }
}
