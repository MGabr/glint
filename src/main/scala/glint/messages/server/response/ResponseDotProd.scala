package glint.messages.server.response

import glint.messages.server.request.Request

/**
  * A response containing dot products
  *
  * @param fPlus dot products of input and output word weights
  * @param fMinus dot products of input and neighbour words
  */
private[glint] case class ResponseDotProd(fPlus: Array[Float], fMinus: Array[Float]) extends Request
