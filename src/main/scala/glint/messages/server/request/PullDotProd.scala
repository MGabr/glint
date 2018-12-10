package glint.messages.server.request

/**
  * A pull dot products request
  *
  * @param wInput The indices of the input words
  * @param wOutput The indices of the output words per input word
  * @param seed The seed for generating random neighbours
  */
private[glint] case class PullDotProd(wInput: Array[Int], wOutput: Array[Array[Int]], seed: Long) extends Request
