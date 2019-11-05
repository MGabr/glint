package glint.models.client.async

import akka.actor.ActorRef
import com.typesafe.config.Config
import glint.messages.server.request.PushVectorInt
import glint.messages.server.response.ResponseInt
import glint.partitioning.Partitioner

/**
  * Asynchronous implementation of a BigVector for integers
  */
private[glint] class AsyncBigVectorInt(partitioner: Partitioner,
                                       models: Array[ActorRef],
                                       config: Config,
                                       keys: Long)
  extends AsyncBigVector[Int, ResponseInt, PushVectorInt](partitioner, models, config, keys) {

  /**
    * Creates a push message from given sequence of keys and values
    *
    * @param id The identifier
    * @param keys The keys
    * @param values The values
    * @return A PushVectorInt message for type V
    */
  @inline
  override protected def toPushMessage(id: Int, keys: Array[Int], values: Array[Int]): PushVectorInt = {
    PushVectorInt(id, keys, values)
  }

  /**
    * Extracts a value from a given response at given index
    *
    * @param response The response
    * @param index The index
    * @return The value
    */
  @inline
  override protected def toValue(response: ResponseInt, index: Int): Int = response.values(index)

}
