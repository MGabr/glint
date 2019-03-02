package glint.util

import spire.implicits.cforRange

import scala.collection.mutable

/**
  * A pool storing float arrays and arrays of float arrays of the same length.
  * It can be used to prevent garbage collection
  *
  * @param arrayLength The length of the stored float array arrays
  * @param length The length of the stored float arrays
  */
private[glint] class FloatArraysArrayPool(arrayLength: Int, length: Int) {

  private val arrays = mutable.Queue[Array[Float]]()
  private val arrayArrays = mutable.Queue[Array[Array[Float]]]()

  /**
    * Gets an array from the pool or creates a new one if there are no arrays left in the pool
    *
    * @return An array of zero values and pool length
    */
  def get(): Array[Float] = {
    if (arrays.nonEmpty) {
      arrays.dequeue()
    } else {
      new Array[Float](length)
    }
  }

  /**
    * Puts a new array to the pool and clears it to zero values.
    *
    * @param array An array of pool length
    */
  def putClear(array: Array[Float]): Unit = {
    cforRange(0 until length)(i => {
      array(i) = 0.0f
    })
    arrays.enqueue(array)
  }

  /**
    * Gets an array of arrays from the pool or creates a new one if there are no arrays left in the pool
    *
    * @return An array of null values and pool length
    */
  def getArray(): Array[Array[Float]] = {
    if (arrayArrays.nonEmpty) {
      arrayArrays.dequeue()
    } else {
      Array.ofDim[Array[Float]](arrayLength)
    }
  }

  /**
    * Puts an array of arrays to the pool. The array values have to be null
    *
    * @param array An array of null values and pool length
    */
  def putArray(array: Array[Array[Float]]): Unit = {
    arrayArrays.enqueue(array)
  }
}
