package glint.serialization

import glint.messages.server.logic._
import glint.messages.server.request._
import glint.messages.server.response._
import org.scalatest.{FlatSpec, Matchers}

/**
  * A serialization spec test
  */
class SerializationSpec extends FlatSpec with Matchers {

  "A LogicSerializer" should "serialize and deserialize a AcknowledgeReceipt" in {
    val logicSerializer = new LogicSerializer()
    val bytes = logicSerializer.toBinary(AcknowledgeReceipt(1))
    val reconstruction = logicSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[AcknowledgeReceipt])
    val acknowledgeReceipt = reconstruction.asInstanceOf[AcknowledgeReceipt]
    acknowledgeReceipt.id should equal(1)
  }

  it should "serialize and deserialize a Forget" in {
    val logicSerializer = new LogicSerializer()
    val bytes = logicSerializer.toBinary(Forget(1))
    val reconstruction = logicSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[Forget])
    val forget = reconstruction.asInstanceOf[Forget]
    forget.id should equal(1)
  }

  it should "serialize and deserialize a GetUniqueID" in {
    val logicSerializer = new LogicSerializer()
    val bytes = logicSerializer.toBinary(GetUniqueID())
    val reconstruction = logicSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[GetUniqueID])
  }

  it should "serialize and deserialize a NotAcknowledgeReceipt" in {
    val logicSerializer = new LogicSerializer()
    val bytes = logicSerializer.toBinary(NotAcknowledgeReceipt(1))
    val reconstruction = logicSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[NotAcknowledgeReceipt])
    val notAcknowledgeReceipt = reconstruction.asInstanceOf[NotAcknowledgeReceipt]
    notAcknowledgeReceipt.id should equal(1)
  }

  it should "serialize and deserialize a UniqueID" in {
    val logicSerializer = new LogicSerializer()
    val bytes = logicSerializer.toBinary(UniqueID(1))
    val reconstruction = logicSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[UniqueID])
    val uniqueID = reconstruction.asInstanceOf[UniqueID]
    uniqueID.id should equal(1)
  }

  "A RequestSerializer" should "serialize and deserialize a PullAverageRows" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullAverageRows(Array(Array(0, 1, 2), Array(), Array(0, 2))))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullAverageRows])
    val pullAverageRow = reconstruction.asInstanceOf[PullAverageRows]
    pullAverageRow.rows should equal(Array(Array(0, 1, 2), Array(), Array(0, 2)))
  }

  it should "serialize and deserialize a PullDotProd" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullDotProd(Array(0, 1, 2), Array(Array(0, 1), Array(0, 1, 2), Array(1, 2)), 1L))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullDotProd])
    val pullDotProd = reconstruction.asInstanceOf[PullDotProd]
    pullDotProd.wInput should equal(Array(0, 1, 2))
    pullDotProd.wOutput should equal(Array(Array(0, 1), Array(0, 1, 2), Array(1, 2)))
    pullDotProd.seed should equal(1L)
  }

  it should "serialize and deserialize a PullDotProdFM" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullDotProdFM(
      Array(Array(0), Array(0), Array(1, 3)),
      Array(Array(1.0f), Array(1.0f), Array(1.0f, 0.25f)),
      Array(Array(4, 6), Array(5), Array(4, 6)),
      Array(Array(1.0f, 0.3f), Array(1.0f), Array(1.0f, 0.3f))
    ))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullDotProdFM])
    val pullDotProd = reconstruction.asInstanceOf[PullDotProdFM]
    pullDotProd.iUser should equal(Array(Array(0), Array(0), Array(1, 3)))
    pullDotProd.wUser should equal(Array(Array(1.0f), Array(1.0f), Array(1.0f, 0.25f)))
    pullDotProd.iItem should equal(Array(Array(4, 6), Array(5), Array(4, 6)))
    pullDotProd.wItem should equal(Array(Array(1.0f, 0.3f), Array(1.0f), Array(1.0f, 0.3f)))
  }

  it should "serialize and deserialize a PullMatrix" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullMatrix(Array(0, 1, 2), Array(3, 4, 5)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullMatrix])
    val pullMatrix = reconstruction.asInstanceOf[PullMatrix]
    pullMatrix.rows should equal(Array(0, 1, 2))
    pullMatrix.cols should equal(Array(3, 4, 5))
  }

  it should "serialize and deserialize a PullMatrixRows" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullMatrixRows(Array(0, 1, 2, 5)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullMatrixRows])
    val pullMatrixRows = reconstruction.asInstanceOf[PullMatrixRows]
    pullMatrixRows.rows should equal(Array(0, 1, 2, 5))
  }

  it should "serialize and deserialize a PullMultiply" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullMultiply(Array(0.2f, 0.1f, 0.3f, 0.0f), 0, 100))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullMultiply])
    val pullMultiply = reconstruction.asInstanceOf[PullMultiply]
    pullMultiply.vector should equal(Array(0.2f, 0.1f, 0.3f, 0.0f))
    pullMultiply.startRow shouldBe 0
    pullMultiply.endRow shouldBe 100
  }

  it should "serialize and deserialize a PullNormDots" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullNormDots(0, 100))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullNormDots])
    val pullNormDots = reconstruction.asInstanceOf[PullNormDots]
    pullNormDots.startRow shouldBe 0
    pullNormDots.endRow shouldBe 100
  }

  it should "serialize and deserialize a PullSumFM" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullSumFM(
      Array(Array(0, 4, 6), Array(0, 5), Array(1, 3, 4, 6)),
      Array(Array(1.0f, 1.0f, 0.3f), Array(1.0f, 1.0f), Array(1.0f, 0.25f, 1.0f, 0.3f))
    ))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullSumFM])
    val pullSumFM = reconstruction.asInstanceOf[PullSumFM]
    pullSumFM.indices should equal(Array(Array(0, 4, 6), Array(0, 5), Array(1, 3, 4, 6)))
    pullSumFM.weights should equal(Array(Array(1.0f, 1.0f, 0.3f), Array(1.0f, 1.0f), Array(1.0f, 0.25f, 1.0f, 0.3f)))
  }

  it should "serialize and deserialize a PullVector" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PullVector(Array(0, 16, 2, 5)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PullVector])
    val pullVector = reconstruction.asInstanceOf[PullVector]
    pullVector.keys should equal(Array(0, 16, 2, 5))
  }

  it should "serialize and deserialize a PushAdjust" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushAdjust(
      2, Array(0, 1, 2), Array(Array(0, 1), Array(0, 1, 2), Array(1, 2)),
      Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f),
      Array(0.0f, 0.01f, 0.1f, 0.11f, 0.2f, 0.21f, 0.3f, 0.31f, 0.4f, 0.41f, 0.5f, 0.51f, 0.6f, 0.61f),
      1L))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushAdjust])
    val pushAdjust = reconstruction.asInstanceOf[PushAdjust]
    pushAdjust.id should equal(2)
    pushAdjust.wInput should equal(Array(0, 1, 2))
    pushAdjust.wOutput should equal(Array(Array(0, 1), Array(0, 1, 2), Array(1, 2)))
    pushAdjust.gPlus should equal(Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f))
    pushAdjust.gMinus should equal(Array(0.0f, 0.01f, 0.1f, 0.11f, 0.2f, 0.21f, 0.3f, 0.31f, 0.4f, 0.41f, 0.5f, 0.51f, 0.6f, 0.61f))
    pushAdjust.seed should equal(1L)
  }

  it should "serialize and deserialize a PushAdjustFM" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushAdjustFM(2, Array(0.1f, 0.2f, 0.3f), 15))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushAdjustFM])
    val pushAdjust = reconstruction.asInstanceOf[PushAdjustFM]
    pushAdjust.id should equal(2)
    pushAdjust.g should equal(Array(0.1f, 0.2f, 0.3f))
    pushAdjust.cacheKey should equal(15)
  }

  it should "serialize and deserialize a PushMatrixDouble" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushMatrixDouble(2, Array(0, 5, 9), Array(2, 10, 3), Array(0.0, 0.5, 0.99)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushMatrixDouble])
    val pushMatrixDouble = reconstruction.asInstanceOf[PushMatrixDouble]
    pushMatrixDouble.rows should equal(Array(0, 5, 9))
    pushMatrixDouble.cols should equal(Array(2, 10, 3))
    pushMatrixDouble.values should equal(Array(0.0, 0.5, 0.99))
  }

  it should "serialize and deserialize a PushMatrixFloat" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushMatrixFloat(32, Array(0, 5, 9), Array(2, 10, 3), Array(0.3f, 0.6f, 10.314f)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushMatrixFloat])
    val pushMatrixFloat = reconstruction.asInstanceOf[PushMatrixFloat]
    pushMatrixFloat.rows should equal(Array(0, 5, 9))
    pushMatrixFloat.cols should equal(Array(2, 10, 3))
    pushMatrixFloat.values should equal(Array(0.3f, 0.6f, 10.314f))
  }

  it should "serialize and deserialize a PushMatrixInt" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushMatrixInt(16, Array(1, 2, 1000000000), Array(10000, 10, 1), Array(99, -20, -3500)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushMatrixInt])
    val pushMatrixInt = reconstruction.asInstanceOf[PushMatrixInt]
    pushMatrixInt.rows should equal(Array(1, 2, 1000000000))
    pushMatrixInt.cols should equal(Array(10000, 10, 1))
    pushMatrixInt.values should equal(Array(99, -20, -3500))
  }

  it should "serialize and deserialize a PushMatrixLong" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushMatrixLong(0, Array(1, 2, 1000000000), Array(10000, 10, 1), Array(5000300200100L, -9000100200300L, 0L)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushMatrixLong])
    val pushMatrixLong = reconstruction.asInstanceOf[PushMatrixLong]
    pushMatrixLong.rows should equal(Array(1, 2, 1000000000))
    pushMatrixLong.cols should equal(Array(10000, 10, 1))
    pushMatrixLong.values should equal(Array(5000300200100L, -9000100200300L, 0L))
  }

  it should "serialize and deserialize a PushVectorDouble" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushVectorDouble(123, Array(0, 5, 9), Array(0.0, 0.5, 0.99)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushVectorDouble])
    val pushMatrixDouble = reconstruction.asInstanceOf[PushVectorDouble]
    pushMatrixDouble.keys should equal(Array(0, 5, 9))
    pushMatrixDouble.values should equal(Array(0.0, 0.5, 0.99))
  }

  it should "serialize and deserialize a PushVectorFloat" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushVectorFloat(9999, Array(0, 5, 9), Array(0.3f, 0.6f, 10.314f)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushVectorFloat])
    val pushMatrixFloat = reconstruction.asInstanceOf[PushVectorFloat]
    pushMatrixFloat.keys should equal(Array(0, 5, 9))
    pushMatrixFloat.values should equal(Array(0.3f, 0.6f, 10.314f))
  }

  it should "serialize and deserialize a PushVectorInt" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushVectorInt(231, Array(1, 2, 1000000000), Array(99, -20, -3500)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushVectorInt])
    val pushMatrixInt = reconstruction.asInstanceOf[PushVectorInt]
    pushMatrixInt.keys should equal(Array(1, 2, 1000000000))
    pushMatrixInt.values should equal(Array(99, -20, -3500))
  }

  it should "serialize and deserialize a PushVectorLong" in {
    val requestSerializer = new RequestSerializer()
    val bytes = requestSerializer.toBinary(PushVectorLong(213, Array(1, 2, 1000000000), Array(5000300200100L, -9000100200300L, 0L)))
    val reconstruction = requestSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[PushVectorLong])
    val pushMatrixLong = reconstruction.asInstanceOf[PushVectorLong]
    pushMatrixLong.keys should equal(Array(1, 2, 1000000000))
    pushMatrixLong.values should equal(Array(5000300200100L, -9000100200300L, 0L))
  }

  "A ResponseSerializer" should "serialize and deserialize a ResponseDouble" in {
    val responseSerializer = new ResponseSerializer()
    val bytes = responseSerializer.toBinary(ResponseDouble(Array(0.01, 3.1415, -0.999)))
    val reconstruction = responseSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[ResponseDouble])
    val responseDouble = reconstruction.asInstanceOf[ResponseDouble]
    responseDouble.values should equal(Array(0.01, 3.1415, -0.999))
  }

  it should "serialize and deserialize a ResponseFloat" in {
    val responseSerializer = new ResponseSerializer()
    val bytes = responseSerializer.toBinary(ResponseFloat(Array(100.001f, -3.1415f, 0.1234f)))
    val reconstruction = responseSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[ResponseFloat])
    val responseFloat = reconstruction.asInstanceOf[ResponseFloat]
    responseFloat.values should equal(Array(100.001f, -3.1415f, 0.1234f))
  }

  it should "serialize and deserialize a ResponseInt" in {
    val responseSerializer = new ResponseSerializer()
    val bytes = responseSerializer.toBinary(ResponseInt(Array(100, -200, 999123)))
    val reconstruction = responseSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[ResponseInt])
    val responseInt = reconstruction.asInstanceOf[ResponseInt]
    responseInt.values should equal(Array(100, -200, 999123))
  }

  it should "serialize and deserialize a ResponseLong" in {
    val responseSerializer = new ResponseSerializer()
    val bytes = responseSerializer.toBinary(ResponseLong(Array(0L, -200L, 9876300200100L)))
    val reconstruction = responseSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[ResponseLong])
    val responseInt = reconstruction.asInstanceOf[ResponseLong]
    responseInt.values should equal(Array(0L, -200L, 9876300200100L))
  }

  it should "serialize and deserialize a ResponseDotProd" in {
    val responseSerializer = new ResponseSerializer()
    val bytes = responseSerializer.toBinary(ResponseDotProd(
      Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f),
      Array(0.0f, 0.01f, 0.1f, 0.11f, 0.2f, 0.21f, 0.3f, 0.31f, 0.4f, 0.41f, 0.5f, 0.51f, 0.6f, 0.61f)))
    val reconstruction = responseSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[ResponseDotProd])
    val responseDotProd = reconstruction.asInstanceOf[ResponseDotProd]
    responseDotProd.fPlus should equal(Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f))
    responseDotProd.fMinus should equal(Array(0.0f, 0.01f, 0.1f, 0.11f, 0.2f, 0.21f, 0.3f, 0.31f, 0.4f, 0.41f, 0.5f, 0.51f, 0.6f, 0.61f))
  }

  it should "serialize and deserialize a ResponseDotProdFM" in {
    val responseSerializer = new ResponseSerializer()
    val bytes = responseSerializer.toBinary(ResponseDotProdFM(
      Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f), 15))
    val reconstruction = responseSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[ResponseDotProdFM])
    val responseDotProdFM = reconstruction.asInstanceOf[ResponseDotProdFM]
    responseDotProdFM.f should equal(Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f))
    responseDotProdFM.cacheKey should equal(15)
  }

  it should "serialize and deserialize a ResponsePullSumFM" in {
    val responseSerializer = new ResponseSerializer()
    val bytes = responseSerializer.toBinary(ResponsePullSumFM(
      Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f), 15))
    val reconstruction = responseSerializer.fromBinary(bytes)
    assert(reconstruction.isInstanceOf[ResponsePullSumFM])
    val responsePullSumFM = reconstruction.asInstanceOf[ResponsePullSumFM]
    responsePullSumFM.s should equal(Array(0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f))
    responsePullSumFM.cacheKey should equal(15)
  }
}
