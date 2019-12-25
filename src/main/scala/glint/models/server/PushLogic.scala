package glint.models.server

import akka.actor.ActorRef
import akka.routing.{RandomRoutingLogic, Routee, RoutingLogic}
import glint.messages.server.logic._
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet

import scala.collection.immutable

/**
  * Encapsulation for common push logic behavior
  */
trait PushLogic {

  /**
    * A set of received message ids
    */
  val receipt: IntHashSet = new IntHashSet()

  /**
    * Unique identifier counter
    */
  var uid = 0

  /**
    * Increases the unique id and returns the next unique id
    *
    * @return The next id
    */
  @inline
  def nextId(): Int = {
    uid += 1
    uid
  }

  /**
    * Handles push message receipt logic
    *
    * @param message The message
    * @param sender The sender
    */
  def handleLogic(message: Any, sender: ActorRef) = message match {
    case GetUniqueID() =>
      sender ! UniqueID(nextId())

    case AcknowledgeReceipt(id) =>
      if (receipt.contains(id)) {
        sender ! AcknowledgeReceipt(id)
      } else {
        sender ! NotAcknowledgeReceipt(id)
      }

    case Forget(id) =>
      if (receipt.contains(id)) {
        receipt.remove(id)
      }
      sender ! Forget(id)
  }

  /**
    * Adds the message id to the received set
    *
    * @param id The message id
    */
  @inline
  def updateFinished(id: Int): Unit = {
    receipt.add(id)
  }

}

/**
 * Encapsulation for common push logic behavior
 * where an additional routee id is included in the unique id for consistent routing to the same routee
 */
trait RouteePushLogic extends PushLogic {

  val routeeId: Int

  override def nextId(): Int = {
    uid = (uid + 1) % Short.MaxValue
    (routeeId << 16) | uid
  }
}

/**
 * Routing logic for push routee actors
 */
private[glint] class PushRoutingLogic extends RoutingLogic {

  private val randomRoutingLogic = RandomRoutingLogic()

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case AcknowledgeReceipt(id) => routees(id >> 16)
      case NotAcknowledgeReceipt(id) => routees(id >> 16)
      case Forget(id) => routees(id >> 16)
      case _ => randomRoutingLogic.select(message, routees)
    }
  }
}