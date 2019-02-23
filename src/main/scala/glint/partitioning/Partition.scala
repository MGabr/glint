package glint.partitioning

import glint.partitioning.by.GlobalLocalConversion

/**
  * An abstract partition
  *
  * @param index The index of this partition
  */
abstract class Partition(val index: Int) extends Serializable with GlobalLocalConversion {

  /**
    * Checks whether given global key falls within this partition
    *
    * @param key The key
    * @return True if the global key falls within this partition, false otherwise
    */
  @inline
  def contains(key: Long): Boolean

  /**
    * Computes the size of this partition
    *
    * @return The size of this partition
    */
  def size: Int

}
