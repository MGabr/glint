package glint.util

import spire.implicits.cforRange

import scala.collection.mutable

/**
  * A pool storing integer arrays of the same length which can be used to prevent garbage collection
  *
  * @param length the length of the stored arrays
  */
private[glint] class IntArrayPool(length: Int) {

  private val arrays = mutable.Queue[Array[Int]]()

  /**
    * Gets an array from the pool or creates a new one if there are no arrays left in the pool
    *
    * @return An array of zero values and pool length
    */
  def get(): Array[Int] = {
    if (arrays.nonEmpty) {
      arrays.dequeue()
    } else {
      new Array[Int](length)
    }
  }

  /**
   * Gets an array with a minimum length from the pool or creates a new one if there are no arrays left in the pool.
   * If arrays of less than the minimum length are encountered they are removed from the pool.
   * This allows the pool to adjust dynamically at the cost of some garbage collection
   *
   * @param minLength the minimum length of the array
   * @return An array of zero values and at least minimum and pool length
   */
  def get(minLength: Int): Array[Int] = {
    if (arrays.nonEmpty) {
      val array = arrays.dequeue()
      if (array.length < minLength) get(minLength) else array
    } else {
      new Array[Int](Math.max(minLength, length))
    }
  }

  /**
    * Puts a new array to the pool. The array values have to be zero or it has to be accepted that arrays with non-zero
    * values will be returned by [[get() get]].
    *
    * @param array An array of pool length
    */
  def put(array: Array[Int]): Unit = {
    arrays.enqueue(array)
  }

  /**
    * Puts a new array to the pool and clears it to zero until the specified index.
    * The array values have to already be zero after the specified index
    *
    * @param array An array of pool length
    * @param clearUntil The index until which the array values are non-zero
    */
  def putClearUntil(array: Array[Int], clearUntil: Int): Unit = {
    cforRange(0 until clearUntil)(j => {
      array(j) = 0
    })
    arrays.enqueue(array)
  }
}
