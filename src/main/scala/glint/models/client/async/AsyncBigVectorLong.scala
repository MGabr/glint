package glint.models.client.async

import akka.actor.ActorRef
import com.typesafe.config.Config
import glint.messages.server.request.PushVectorLong
import glint.messages.server.response.ResponseLong
import glint.partitioning.Partitioner

/**
  * Asynchronous implementation of a BigVector for longs
  */
private[glint] class AsyncBigVectorLong(partitioner: Partitioner,
                                        models: Array[ActorRef],
                                        config: Config,
                                        keys: Long)
  extends AsyncBigVector[Long, ResponseLong, PushVectorLong](partitioner, models, config, keys) {

  /**
    * Creates a push message from given sequence of keys and values
    *
    * @param id The identifier
    * @param keys The keys
    * @param values The values
    * @return A PushVectorLong message for type V
    */
  @inline
  override protected def toPushMessage(id: Int, keys: Array[Int], values: Array[Long]): PushVectorLong = {
    PushVectorLong(id, keys, values)
  }

  /**
    * Extracts a value from a given response at given index
    *
    * @param response The response
    * @param index The index
    * @return The value
    */
  @inline
  override protected def toValue(response: ResponseLong, index: Int): Long = response.values(index)

}
