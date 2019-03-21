package glint.util

import spire.implicits.cforRange

import scala.collection.mutable

/**
  * A pool storing arrays of float arrays of the same length.
  * It can be used to prevent garbage collection
  *
  * @param array The length of the stored float array arrays
  */
private[glint] class FloatArraysArrayPool(length: Int) {

  private val arrays = mutable.Queue[Array[Array[Float]]]()

  /**
    * Gets an array of arrays from the pool or creates a new one if there are no arrays left in the pool
    *
    * @return An array of null values and pool length
    */
  def get(): Array[Array[Float]] = {
    if (arrays.nonEmpty) {
      arrays.dequeue()
    } else {
      Array.ofDim[Array[Float]](length)
    }
  }

  /**
    * Puts an array of arrays to the pool. The array values have to be null
    *
    * @param array An array of null values and pool length
    */
  def put(array: Array[Array[Float]]): Unit = {
    arrays.enqueue(array)
  }
}
