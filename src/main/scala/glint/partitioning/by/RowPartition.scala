package glint.partitioning.by

/**
  * A row partition
  */
private[partitioning] trait RowPartition extends HasGlobalToLocal {

  /**
    * Converts given global column key to a continuous local array index [0, 1, ...]
    *
    * @param key The global column key
    * @return The local index
    */
  @inline
  override def globalColToLocal(key: Long): Int = key.toInt

}
