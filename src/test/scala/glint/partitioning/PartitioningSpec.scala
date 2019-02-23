package glint.partitioning

import glint.partitioning.by.PartitionBy
import glint.partitioning.cyclic.CyclicPartitioner
import glint.partitioning.range.RangePartitioner
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

/**
  * Partitioning test specification
  */
class PartitioningSpec extends FlatSpec {

  /**
    * Function that asserts whether given key is only in the assigned partition
    *
    * @param key The key
    * @param partitions All the partitions
    * @param assigned The assigned partition
    */
  private def assertKeyInCorrectPartition(key: Long, partitions: Array[Partition], assigned: Partition): Unit = {
    var i = 0
    while (i < partitions.length) {
      if (partitions(i) == assigned) {
        assert(partitions(i).contains(key))
      } else {
        assert(!partitions(i).contains(key))
      }
      i += 1
    }
  }

  /**
    * Function that asserts whether the given keys are same as before after being partitioned and inversed
    *
    * @param keys The keys
    * @param partitioner The partitioner
    */
  private def assertPartitionedAndInversedKeysSame(keys: Range, partitioner: Partitioner): Unit = {
    val partitions = keys.map(partitioner.partition(_))

    val localRowIndices = keys.zip(partitions).map { case (k, p) => p.globalRowToLocal(k) }
    val localColIndices = keys.zip(partitions).map { case (k, p) => p.globalColToLocal(k) }

    val globalRowIndices = localRowIndices.zip(partitions).map { case (i, p) => p.localRowToGlobal(i) }
    val globalColIndices = localColIndices.zip(partitions).map { case (i, p) => p.localColToGlobal(i) }

    globalRowIndices should equal(keys)
    globalColIndices should equal(keys)
  }

  "A CyclicPartitioner" should "partition all its keys to partitions that contain it" in {
    val partitioner = CyclicPartitioner(5, 37)
    for (key <- 0 until 37) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "partition an uneven distribution" in {
    val partitioner = CyclicPartitioner(20, 50)
    for (key <- 0 until 50) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "succeed with the same number of partitions and keys" in {
    val partitioner = CyclicPartitioner(13, 13)
    for (key <- 0 until 13) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "succeed with more partitions than keys" in {
    val partitioner = CyclicPartitioner(33, 12)
    for (key <- 0 until 12) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "guarantee unique places within each partition" in {
    val partitioner = CyclicPartitioner(17, 137)
    val partitions = partitioner.all()
    for (partition <- partitions) {
      val data = Array.fill[Boolean](173)(false)
      for (key <- 0 until 137) {
        if (partition.contains(key)) {
          val index = partition.globalRowToLocal(key)
          assert(!data(index))
          data(index) = true
        }
      }
    }
  }

  it should "row partition by default" in {
    val partitioner = CyclicPartitioner(5, 37)

    // after first cycle local and global row indices should differ
    for (key <- 5 until 37) {
      val partition = partitioner.partition(key)
      val rowIndex = partition.globalRowToLocal(key)
      assert(rowIndex != key)
      val colIndex = partition.globalColToLocal(key)
      assert(colIndex == key)
    }
  }

  it should "column partition when specified" in {
    val partitioner = CyclicPartitioner(5, 37, PartitionBy.COL)

    // after first cycle local and global column indices should differ
    for (key <- 5 until 37) {
      val partition = partitioner.partition(key)
      val rowIndex = partition.globalRowToLocal(key)
      assert(rowIndex == key)
      val colIndex = partition.globalColToLocal(key)
      assert(colIndex != key)
    }
  }

  it should "provide inverse row partitioning function" in {
    val partitioner = CyclicPartitioner(5, 37, PartitionBy.ROW)
    val keys = 0 until 37

    assertPartitionedAndInversedKeysSame(keys, partitioner)
  }

  it should "provide inverse column partitioning function" in {
    val partitioner = CyclicPartitioner(5, 37, PartitionBy.COL)
    val keys = 0 until 37

    assertPartitionedAndInversedKeysSame(keys, partitioner)
  }

  it should "fail when attempting to partition keys outside its key space" in {
    val partitioner = CyclicPartitioner(13, 105)
    an[IndexOutOfBoundsException] should be thrownBy partitioner.partition(105)
    an[IndexOutOfBoundsException] should be thrownBy partitioner.partition(-2)
  }

  "A RangePartitioner" should "partition all its keys into partitions that contain it" in {
    val partitioner = RangePartitioner(5, 109)
    for (key <- 0 until 109) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "partition an uneven distribution" in {
    val partitioner = RangePartitioner(20, 50)
    for (key <- 0 until 50) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "succeed with the same number of partitions and keys" in {
    val partitioner = RangePartitioner(13, 13)
    for (key <- 0 until 13) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "succeed with more partitions than keys" in {
    val partitioner = RangePartitioner(33, 12)
    for (key <- 0 until 12) {
      val partition = partitioner.partition(key)
      assertKeyInCorrectPartition(key, partitioner.all(), partition)
    }
  }

  it should "guarantee unique places within each partition" in {
    val partitioner = RangePartitioner(15, 97)
    val partitions = partitioner.all()
    for (partition <- partitions) {
      val data = Array.fill[Boolean](97)(false)
      for (key <- 0 until 97) {
        if (partition.contains(key)) {
          val index = partition.globalRowToLocal(key)
          assert(!data(index))
          data(index) = true
        }
      }
    }
  }

  it should "support row partitioning by default" in {
    val partitioner = RangePartitioner(5, 100)

    // after second partition local and global row indices should differ
    for (key <- 20 until 100) {
      val partition = partitioner.partition(key)
      val rowIndex = partition.globalRowToLocal(key)
      assert(rowIndex != key)
      val colIndex = partition.globalColToLocal(key)
      assert(colIndex == key)
    }
  }

  it should "column partition when specified" in {
    val partitioner = RangePartitioner(5, 100, PartitionBy.COL)

    // after second partition local and global column indices should differ
    for (key <- 20 until 100) {
      val partition = partitioner.partition(key)
      val rowIndex = partition.globalRowToLocal(key)
      assert(rowIndex == key)
      val colIndex = partition.globalColToLocal(key)
      assert(colIndex != key)
    }
  }

  it should "provide inverse row partitioning function" in {
    val partitioner = RangePartitioner(5, 100, PartitionBy.ROW)
    val keys = 5 until 100

    assertPartitionedAndInversedKeysSame(keys, partitioner)
  }

  it should "provide inverse column partitioning function" in {
    val partitioner = RangePartitioner(5, 100, PartitionBy.COL)
    val keys = 5 until 100

    assertPartitionedAndInversedKeysSame(keys, partitioner)
  }

  it should "fail when attempting to partition keys outside its key space" in {
    val partitioner = RangePartitioner(12, 105)
    an[IndexOutOfBoundsException] should be thrownBy partitioner.partition(105)
    an[IndexOutOfBoundsException] should be thrownBy partitioner.partition(-2)
  }

}