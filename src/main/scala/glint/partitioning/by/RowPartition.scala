package glint.partitioning.by

/**
  * A row partition
  */
private[partitioning] trait RowPartition extends GlobalLocalConversion {

  @inline
  override def globalColToLocal(key: Long): Int = key.toInt

  @inline
  override def localColToGlobal(index: Int): Long = index.toLong
}
