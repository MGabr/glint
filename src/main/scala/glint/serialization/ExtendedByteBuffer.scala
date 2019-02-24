package glint.serialization

import java.nio.ByteBuffer
import spire.implicits.cforRange

class ExtendedByteBuffer(val buf: ByteBuffer) {

  def putLongArray(values: Array[Long]): Unit = {
    val typedBuffer = buf.asLongBuffer()
    typedBuffer.put(values)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfLong)
  }

  def putLongArrayArray(values: Array[Array[Long]]): Unit = {
    val typedBuffer = buf.asLongBuffer()
    cforRange(0 until values.length) { i =>
      typedBuffer.put(values(i).length)
      typedBuffer.put(values(i))
    }
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfLong)
  }

  def putIntArray(values: Array[Int]): Unit = {
    val typedBuffer = buf.asIntBuffer()
    typedBuffer.put(values)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfInt)
  }

  def putDoubleArray(values: Array[Double]): Unit = {
    val typedBuffer = buf.asDoubleBuffer()
    typedBuffer.put(values)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfDouble)
  }

  def putFloatArray(values: Array[Float]): Unit = {
    val typedBuffer = buf.asFloatBuffer()
    typedBuffer.put(values)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfFloat)
  }

  def putIntArrayArray(values: Array[Array[Int]]): Unit = {
    val typedBuffer = buf.asIntBuffer()
    cforRange(0 until values.length) { i =>
      typedBuffer.put(values(i).length)
      typedBuffer.put(values(i))
    }
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfInt)
  }

  def getLongArray(size: Int): Array[Long] = {
    val output = new Array[Long](size)
    val typedBuffer = buf.asLongBuffer()
    typedBuffer.get(output)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfLong)
    output
  }

  def getIntArray(size: Int): Array[Int] = {
    val output = new Array[Int](size)
    val typedBuffer = buf.asIntBuffer()
    typedBuffer.get(output)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfInt)
    output
  }

  def getDoubleArray(size: Int): Array[Double] = {
    val output = new Array[Double](size)
    val typedBuffer = buf.asDoubleBuffer()
    typedBuffer.get(output)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfDouble)
    output
  }

  def getFloatArray(size: Int): Array[Float] = {
    val output = new Array[Float](size)
    val typedBuffer = buf.asFloatBuffer()
    typedBuffer.get(output)
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfFloat)
    output
  }

  def getIntArrayArray(sizeArray: Int): Array[Array[Int]] = {
    val output = new Array[Array[Int]](sizeArray)
    val typedBuffer = buf.asIntBuffer()
    cforRange(0 until sizeArray) { i =>
      output(i) = new Array[Int](typedBuffer.get())
      typedBuffer.get(output(i))
    }
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfInt)
    output
  }

  def getLongArrayArray(sizeArray: Int): Array[Array[Long]] = {
    val output = new Array[Array[Long]](sizeArray)
    val typedBuffer = buf.asLongBuffer()
    cforRange(0 until sizeArray) { i =>
      output(i) = new Array[Long](typedBuffer.get().toInt)
      typedBuffer.get(output(i))
    }
    buf.position(buf.position() + typedBuffer.position() * SerializationConstants.sizeOfLong)
    output
  }
}

object ExtendedByteBuffer {
  implicit def byteBufferToExtendedByteBuffer(b: ByteBuffer): ExtendedByteBuffer = new ExtendedByteBuffer(b)
}
