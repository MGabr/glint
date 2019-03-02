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
