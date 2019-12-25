package glint.serialization

import java.nio.ByteBuffer

import glint.messages.server.request._
import glint.serialization.ExtendedByteBuffer._

/**
  * A fast serializer for requests
  *
  * Internally this uses byte buffers for fast serialization and deserialization.
  */
class RequestSerializer extends GlintSerializer {

  override def identifier: Int = 13370

  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = {
    o match {
      case x: PullAverageRows =>
        buf.put(SerializationConstants.pullAverageRowsByte)
        buf.putInt(x.rows.length)
        buf.putIntArrayArray(x.rows)

      case x: PullDotProd =>
        buf.put(SerializationConstants.pullDotProdByte)
        buf.putInt(x.wInput.length)
        buf.putIntArray(x.wInput)
        buf.putIntArrayArray(x.wOutput)
        buf.putLong(x.seed)

      case x: PullDotProdFM =>
        if (x.cache) {
          buf.put(SerializationConstants.pullDotProdFMCacheByte)
        } else {
          buf.put(SerializationConstants.pullDotProdFMByte)
        }
        buf.putInt(x.iUser.length)
        buf.putIntArrayArray(x.iUser)
        buf.putFloatArrayArray(x.wUser)
        buf.putIntArrayArray(x.iItem)
        buf.putFloatArrayArray(x.wItem)

      case x: PullMatrix =>
        buf.put(SerializationConstants.pullMatrixByte)
        buf.putInt(x.rows.length)
        buf.putIntArray(x.rows)
        buf.putIntArray(x.cols)

      case x: PullMatrixRows =>
        buf.put(SerializationConstants.pullMatrixRowsByte)
        buf.putInt(x.rows.length)
        buf.putIntArray(x.rows)

      case x: PullMultiply =>
        buf.put(SerializationConstants.pullMultiplyByte)
        buf.putInt(x.vector.length)
        buf.putFloatArray(x.vector)
        buf.putInt(x.startRow)
        buf.putInt(x.endRow)

      case x: PullNormDots =>
        buf.put(SerializationConstants.pullNormDotsByte)
        buf.putInt(0) // dummy value
        buf.putInt(x.startRow)
        buf.putInt(x.endRow)

      case x: PullSumFM =>
        if (x.cache) {
          buf.put(SerializationConstants.pullSumFMCacheByte)
        } else {
          buf.put(SerializationConstants.pullSumFMByte)
        }
        buf.putInt(x.indices.length)
        buf.putIntArrayArray(x.indices)
        buf.putFloatArrayArray(x.weights)

      case x: PullVector =>
        buf.put(SerializationConstants.pullVectorByte)
        buf.putInt(x.keys.length)
        buf.putIntArray(x.keys)

      case x: PushAdjust =>
        buf.put(SerializationConstants.pushAdjustByte)
        buf.putInt(x.gPlus.length)
        buf.putInt(x.gMinus.length)
        buf.putInt(x.id)
        buf.putFloatArray(x.gPlus)
        buf.putFloatArray(x.gMinus)

      case x: PushAdjustFM =>
        buf.put(SerializationConstants.pushAdjustFMByte)
        buf.putInt(x.g.length)
        buf.putInt(x.id)
        buf.putFloatArray(x.g)

      case x: PushMatrixDouble =>
        buf.put(SerializationConstants.pushMatrixDoubleByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putIntArray(x.rows)
        buf.putIntArray(x.cols)
        buf.putDoubleArray(x.values)

      case x: PushMatrixFloat =>
        buf.put(SerializationConstants.pushMatrixFloatByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putIntArray(x.rows)
        buf.putIntArray(x.cols)
        buf.putFloatArray(x.values)

      case x: PushMatrixInt =>
        buf.put(SerializationConstants.pushMatrixIntByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putIntArray(x.rows)
        buf.putIntArray(x.cols)
        buf.putIntArray(x.values)

      case x: PushMatrixLong =>
        buf.put(SerializationConstants.pushMatrixLongByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putIntArray(x.rows)
        buf.putIntArray(x.cols)
        buf.putLongArray(x.values)

      case x: PushSumFM =>
        buf.put(SerializationConstants.pushSumFMByte)
        buf.putInt(x.g.length)
        buf.putInt(x.id)
        buf.putFloatArray(x.g)

      case x: PushVectorDouble =>
        buf.put(SerializationConstants.pushVectorDoubleByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putIntArray(x.keys)
        buf.putDoubleArray(x.values)

      case x: PushVectorFloat =>
        buf.put(SerializationConstants.pushVectorFloatByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putIntArray(x.keys)
        buf.putFloatArray(x.values)

      case x: PushVectorInt =>
        buf.put(SerializationConstants.pushVectorIntByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putIntArray(x.keys)
        buf.putIntArray(x.values)

      case x: PushVectorLong =>
        buf.put(SerializationConstants.pushVectorLongByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putIntArray(x.keys)
        buf.putLongArray(x.values)
    }
  }

  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = {
    val objectType = buf.get()
    val objectSize = buf.getInt()

    objectType match {
      case SerializationConstants.pullAverageRowsByte =>
        val keys = buf.getIntArrayArray(objectSize)
        PullAverageRows(keys)

      case SerializationConstants.pullDotProdByte =>
        val wInput = buf.getIntArray(objectSize)
        val wOutput = buf.getIntArrayArray(objectSize)
        val seed = buf.getLong()
        PullDotProd(wInput, wOutput, seed)

      case SerializationConstants.pullDotProdFMByte =>
        val iUser = buf.getIntArrayArray(objectSize)
        val wUser = buf.getFloatArrayArray(objectSize)
        val iItem = buf.getIntArrayArray(objectSize)
        val wItem = buf.getFloatArrayArray(objectSize)
        PullDotProdFM(iUser, wUser, iItem, wItem, false)

      case SerializationConstants.pullDotProdFMCacheByte =>
        val iUser = buf.getIntArrayArray(objectSize)
        val wUser = buf.getFloatArrayArray(objectSize)
        val iItem = buf.getIntArrayArray(objectSize)
        val wItem = buf.getFloatArrayArray(objectSize)
        PullDotProdFM(iUser, wUser, iItem, wItem, true)

      case SerializationConstants.pullMatrixByte =>
        val rows = buf.getIntArray(objectSize)
        val cols = buf.getIntArray(objectSize)
        PullMatrix(rows, cols)

      case SerializationConstants.pullMatrixRowsByte =>
        val rows = buf.getIntArray(objectSize)
        PullMatrixRows(rows)

      case SerializationConstants.pullMultiplyByte =>
        val vector = buf.getFloatArray(objectSize)
        val startRow = buf.getInt()
        val endRow = buf.getInt()
        PullMultiply(vector, startRow, endRow)

      case SerializationConstants.pullNormDotsByte =>
        val startRow = buf.getInt()
        val endRow = buf.getInt()
        PullNormDots(startRow, endRow)

      case SerializationConstants.pullSumFMByte =>
        val indices = buf.getIntArrayArray(objectSize)
        val weights = buf.getFloatArrayArray(objectSize)
        PullSumFM(indices, weights, false)

      case SerializationConstants.pullSumFMCacheByte =>
        val indices = buf.getIntArrayArray(objectSize)
        val weights = buf.getFloatArrayArray(objectSize)
        PullSumFM(indices, weights, true)

      case SerializationConstants.pullVectorByte =>
        val keys = buf.getIntArray(objectSize)
        PullVector(keys)

      case SerializationConstants.pushAdjustByte =>
        val gPlusSize = objectSize
        val gMinusSize = buf.getInt()
        val id = buf.getInt()
        val gPlus = buf.getFloatArray(gPlusSize)
        val gMinus = buf.getFloatArray(gMinusSize)
        PushAdjust(id, gPlus, gMinus)

      case SerializationConstants.pushAdjustFMByte =>
        val id = buf.getInt()
        val g = buf.getFloatArray(objectSize)
        PushAdjustFM(id, g)

      case SerializationConstants.pushMatrixDoubleByte =>
        val id = buf.getInt()
        val rows = buf.getIntArray(objectSize)
        val cols = buf.getIntArray(objectSize)
        val values = buf.getDoubleArray(objectSize)
        PushMatrixDouble(id, rows, cols, values)

      case SerializationConstants.pushMatrixFloatByte =>
        val id = buf.getInt()
        val rows = buf.getIntArray(objectSize)
        val cols = buf.getIntArray(objectSize)
        val values = buf.getFloatArray(objectSize)
        PushMatrixFloat(id, rows, cols, values)

      case SerializationConstants.pushMatrixIntByte =>
        val id = buf.getInt()
        val rows = buf.getIntArray(objectSize)
        val cols = buf.getIntArray(objectSize)
        val values = buf.getIntArray(objectSize)
        PushMatrixInt(id, rows, cols, values)

      case SerializationConstants.pushMatrixLongByte =>
        val id = buf.getInt()
        val rows = buf.getIntArray(objectSize)
        val cols = buf.getIntArray(objectSize)
        val values = buf.getLongArray(objectSize)
        PushMatrixLong(id, rows, cols, values)

      case SerializationConstants.pushSumFMByte =>
        val id = buf.getInt()
        val g = buf.getFloatArray(objectSize)
        PushSumFM(id, g)

      case SerializationConstants.pushVectorDoubleByte =>
        val id = buf.getInt()
        val keys = buf.getIntArray(objectSize)
        val values = buf.getDoubleArray(objectSize)
        PushVectorDouble(id, keys, values)

      case SerializationConstants.pushVectorFloatByte =>
        val id = buf.getInt()
        val keys = buf.getIntArray(objectSize)
        val values = buf.getFloatArray(objectSize)
        PushVectorFloat(id, keys, values)

      case SerializationConstants.pushVectorIntByte =>
        val id = buf.getInt()
        val keys = buf.getIntArray(objectSize)
        val values = buf.getIntArray(objectSize)
        PushVectorInt(id, keys, values)

      case SerializationConstants.pushVectorLongByte =>
        val id = buf.getInt()
        val keys = buf.getIntArray(objectSize)
        val values = buf.getLongArray(objectSize)
        PushVectorLong(id, keys, values)
    }
  }

}
