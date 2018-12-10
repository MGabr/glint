package glint.messages.server.request

/**
  * A pull dot products request
  *
  * @param id The push identification
  * @param wInput The indices of the input words
  * @param wOutput The indices of the output words per input word
  * @param gPlus The gradient updates for the input and output word combinations
  * @param gMinus The gradient updates for the input and random neighbour word combinations
  * @param seed The seed for generating random neighbours
  */
private[glint] case class PushAdjust(id: Integer,
                                     wInput: Array[Int],
                                     wOutput: Array[Array[Int]],
                                     gPlus: Array[Float],
                                     gMinus: Array[Float],
                                     seed: Long) extends Request
