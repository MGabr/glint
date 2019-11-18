package glint.util

import spire.implicits.cforRange

import scala.collection.mutable

/**
  * A pool storing float arrays of the same length which can be used to prevent garbage collection
  *
  * @param length the length of the stored arrays
  */
private[glint] class FloatArrayPool(length: Int) {

  private val arrays = mutable.Queue[Array[Float]]()

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
   * Puts a new array to the pool. The array values have to be zero or it has to be accepted that arrays with non-zero
   * values will be returned by [[get() get]].
   *
   * @param array An array of pool length
   */
  def put(array: Array[Float]): Unit = {
    arrays.enqueue(array)
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
}
