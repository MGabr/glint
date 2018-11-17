package glint.partitioning.by

private[partitioning] trait HasGlobalToLocal {

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

}
