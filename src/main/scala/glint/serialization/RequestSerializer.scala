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
      case x: PullAverageRow =>
        buf.put(SerializationConstants.pullAverageRow)
        buf.putInt(x.rows.length)
        buf.putLongArray(x.rows)

      case x: PullDotProd =>
        buf.put(SerializationConstants.pullDotProdByte)
        buf.putInt(x.wInput.length)
        buf.putIntArray(x.wInput)
        buf.putIntArrayArray(x.wOutput)
        buf.putLong(x.seed)

      case x: PullMatrix =>
        buf.put(SerializationConstants.pullMatrixByte)
        buf.putInt(x.rows.length)
        buf.putLongArray(x.rows)
        buf.putLongArray(x.cols)

      case x: PullMatrixRows =>
        buf.put(SerializationConstants.pullMatrixRowsByte)
        buf.putInt(x.rows.length)
        buf.putLongArray(x.rows)

      case x: PullMultiply =>
        buf.put(SerializationConstants.pullMultiply)
        buf.putInt(x.vector.length)
        buf.putFloatArray(x.vector)

      case x: PullNormDots =>
        buf.put(SerializationConstants.pullNormDots)
        buf.putInt(0)  // dummy value

      case x: PullVector =>
        buf.put(SerializationConstants.pullVectorByte)
        buf.putInt(x.keys.length)
        buf.putLongArray(x.keys)

      case x: PushAdjust =>
        buf.put(SerializationConstants.pushAdjustByte)
        buf.putInt(x.wInput.length)
        buf.putInt(x.gPlus.length)
        buf.putInt(x.gMinus.length)
        buf.putInt(x.id)
        buf.putIntArray(x.wInput)
        buf.putIntArrayArray(x.wOutput)
        buf.putFloatArray(x.gPlus)
        buf.putFloatArray(x.gMinus)
        buf.putLong(x.seed)

      case x: PushMatrixDouble =>
        buf.put(SerializationConstants.pushMatrixDoubleByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putLongArray(x.rows)
        buf.putLongArray(x.cols)
        buf.putDoubleArray(x.values)

      case x: PushMatrixFloat =>
        buf.put(SerializationConstants.pushMatrixFloatByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putLongArray(x.rows)
        buf.putLongArray(x.cols)
        buf.putFloatArray(x.values)

      case x: PushMatrixInt =>
        buf.put(SerializationConstants.pushMatrixIntByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putLongArray(x.rows)
        buf.putLongArray(x.cols)
        buf.putIntArray(x.values)

      case x: PushMatrixLong =>
        buf.put(SerializationConstants.pushMatrixLongByte)
        buf.putInt(x.rows.length)
        buf.putInt(x.id)
        buf.putLongArray(x.rows)
        buf.putLongArray(x.cols)
        buf.putLongArray(x.values)

      case x: PushVectorDouble =>
        buf.put(SerializationConstants.pushVectorDoubleByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putLongArray(x.keys)
        buf.putDoubleArray(x.values)

      case x: PushVectorFloat =>
        buf.put(SerializationConstants.pushVectorFloatByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putLongArray(x.keys)
        buf.putFloatArray(x.values)

      case x: PushVectorInt =>
        buf.put(SerializationConstants.pushVectorIntByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putLongArray(x.keys)
        buf.putIntArray(x.values)

      case x: PushVectorLong =>
        buf.put(SerializationConstants.pushVectorLongByte)
        buf.putInt(x.keys.length)
        buf.putInt(x.id)
        buf.putLongArray(x.keys)
        buf.putLongArray(x.values)
    }
  }

  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = {
    val objectType = buf.get()
    val objectSize = buf.getInt()

    objectType match {
      case SerializationConstants.pullAverageRow =>
        val keys = buf.getLongArray(objectSize)
        PullAverageRow(keys)

      case SerializationConstants.pullDotProdByte =>
        val wInput = buf.getIntArray(objectSize)
        val wOutput = buf.getIntArrayArray(objectSize)
        val seed = buf.getLong()
        PullDotProd(wInput, wOutput, seed)

      case SerializationConstants.pullMatrixByte =>
        val rows = buf.getLongArray(objectSize)
        val cols = buf.getLongArray(objectSize)
        PullMatrix(rows, cols)

      case SerializationConstants.pullMatrixRowsByte =>
        val rows = buf.getLongArray(objectSize)
        PullMatrixRows(rows)

      case SerializationConstants.pullMultiply =>
        val vector = buf.getFloatArray(objectSize)
        PullMultiply(vector)

      case SerializationConstants.pullNormDots =>
        PullNormDots()

      case SerializationConstants.pullVectorByte =>
        val keys = buf.getLongArray(objectSize)
        PullVector(keys)

      case SerializationConstants.pushAdjustByte =>
        val gPlusSize = buf.getInt()
        val gMinusSize = buf.getInt()
        val id = buf.getInt()
        val wInput = buf.getIntArray(objectSize)
        val wOutput = buf.getIntArrayArray(objectSize)
        val gPlus = buf.getFloatArray(gPlusSize)
        val gMinus = buf.getFloatArray(gMinusSize)
        val seed = buf.getLong()
        PushAdjust(id, wInput, wOutput, gPlus, gMinus, seed)

      case SerializationConstants.pushMatrixDoubleByte =>
        val id = buf.getInt()
        val rows = buf.getLongArray(objectSize)
        val cols = buf.getLongArray(objectSize)
        val values = buf.getDoubleArray(objectSize)
        PushMatrixDouble(id, rows, cols, values)

      case SerializationConstants.pushMatrixFloatByte =>
        val id = buf.getInt()
        val rows = buf.getLongArray(objectSize)
        val cols = buf.getLongArray(objectSize)
        val values = buf.getFloatArray(objectSize)
        PushMatrixFloat(id, rows, cols, values)

      case SerializationConstants.pushMatrixIntByte =>
        val id = buf.getInt()
        val rows = buf.getLongArray(objectSize)
        val cols = buf.getLongArray(objectSize)
        val values = buf.getIntArray(objectSize)
        PushMatrixInt(id, rows, cols, values)

      case SerializationConstants.pushMatrixLongByte =>
        val id = buf.getInt()
        val rows = buf.getLongArray(objectSize)
        val cols = buf.getLongArray(objectSize)
        val values = buf.getLongArray(objectSize)
        PushMatrixLong(id, rows, cols, values)

      case SerializationConstants.pushVectorDoubleByte =>
        val id = buf.getInt()
        val keys = buf.getLongArray(objectSize)
        val values = buf.getDoubleArray(objectSize)
        PushVectorDouble(id, keys, values)

      case SerializationConstants.pushVectorFloatByte =>
        val id = buf.getInt()
        val keys = buf.getLongArray(objectSize)
        val values = buf.getFloatArray(objectSize)
        PushVectorFloat(id, keys, values)

      case SerializationConstants.pushVectorIntByte =>
        val id = buf.getInt()
        val keys = buf.getLongArray(objectSize)
        val values = buf.getIntArray(objectSize)
        PushVectorInt(id, keys, values)

      case SerializationConstants.pushVectorLongByte =>
        val id = buf.getInt()
        val keys = buf.getLongArray(objectSize)
        val values = buf.getLongArray(objectSize)
        PushVectorLong(id, keys, values)
    }
  }

}
