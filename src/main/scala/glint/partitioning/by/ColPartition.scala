package glint.partitioning.by

/**
  * A column partition
  */
private[partitioning] trait ColPartition extends HasGlobalToLocal {

  /**
    * Converts given global row key to a continuous local array index [0, 1, ...]
    *
    * @param key The global row key
    * @return The local index
    */
  @inline
  override def globalRowToLocal(key: Long): Int = key.toInt

}
