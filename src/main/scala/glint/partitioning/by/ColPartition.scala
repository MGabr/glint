package glint.partitioning.by

/**
  * A column partition
  */
private[partitioning] trait ColPartition extends GlobalLocalConversion {

  @inline
  override def globalRowToLocal(key: Long): Int = key.toInt

  @inline
  override def localRowToGlobal(index: Int): Long = index.toLong
}
