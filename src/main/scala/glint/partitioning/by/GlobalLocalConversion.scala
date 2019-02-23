package glint.partitioning.by

/**
  * A trait for conversion between global and local indices
  */
private[partitioning] trait GlobalLocalConversion {

  /**
    * Converts given global row key to a continuous local array index [0, 1, ...]
    *
    * @param key The global row key
    * @return The local index
    */
  @inline
  def globalRowToLocal(key: Long): Int

  /**
    * Converts given global column key to a continuous local array index [0, 1, ...]
    *
    * @param key The global column key
    * @return The local index
    */
  @inline
  def globalColToLocal(key: Long): Int

  /**
    * Converts given row index in a continuous local array [0, 1, ...] to a global key
    *
    * @param index The local row index
    * @return The global key
    */
  @inline
  def localRowToGlobal(index: Int): Long

  /**
    * Converts given column index in a continuous local array [0, 1, ...] to a global key
    *
    * @param index The local column index
    * @return The global key
    */
  @inline
  def localColToGlobal(index: Int): Long
}
