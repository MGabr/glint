package glint.matrix

import akka.actor.ActorSystem
import glint.SystemTest
import glint.models.server.aggregate.AggregateAdd
import glint.models.server.{PartialMatrix, PartialMatrixDouble}
import glint.partitioning.cyclic.CyclicPartition
import glint.partitioning.range.RangePartition
import akka.testkit.TestActorRef
import glint.partitioning.by.PartitionBy
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.{Bench, Gen}

/**
  * Benchmark for matrices
  */
class MatrixBenchmark extends Bench.OfflineReport {

  // Configuration
  override lazy val executor = LocalExecutor(new Executor.Warmer.Default, aggregator, measurer)
  exec.reinstantiation.frequency -> 4

  // Construct necessary data
  implicit val system = ActorSystem("MatrixBenchmark")
  val random = new scala.util.Random(42)
  val rangePartition = RangePartition(1, 10000, 20000, PartitionBy.ROW) // 10000 elements
  val cyclicPartition = CyclicPartition(3, 10, 100000, PartitionBy.ROW) // 10000 elements

  // Construct matrices for range and cyclic partitions
  val rangeMatrixDoubleRef = TestActorRef(new PartialMatrixDouble(rangePartition, 10000, 300, AggregateAdd(), None, None))
  val rangeMatrixDouble = rangeMatrixDoubleRef.underlyingActor
  val cyclicMatrixDoubleRef = TestActorRef(new PartialMatrixDouble(cyclicPartition, 10000, 300, AggregateAdd(), None, None))
  val cyclicMatrixDouble = cyclicMatrixDoubleRef.underlyingActor

  // Sizes and data
  val sizes = Gen.range("size")(4000, 10000, 2000)
  val rangeData = for (size <- sizes) yield {
    val rows = (0L until size).map { case x => x + 10000 }.toArray
    val cols = (0L until size).map { case x => x % 300 }.toArray
    val values = (0 until size).map(_ => random.nextDouble()).toArray
    (rows, cols, values)
  }
  val cyclicData = for (size <- sizes) yield {
    val rows = (0L until size).map { case x => (x * 10) + 3 }.toArray
    val cols = (0L until size).map { case x => x % 300 }.toArray
    val values = (0 until size).map(_ => random.nextDouble()).toArray
    (rows, cols, values)
  }

  // Number of benchmark runs
  val benchRuns = 60

  performance of "RangePartialMatrixDouble" in {
    measure method "update" config (
      exec.benchRuns -> benchRuns
      ) in {
      using(rangeData) in { case (rows, cols, values) =>
        rangeMatrixDouble.update(rows, cols, values)
      }
    }

    measure method "get" config (
      exec.benchRuns -> benchRuns
      ) in {
      using(rangeData) in { case (rows, cols, values) =>
        rangeMatrixDouble.get(rows, cols)
      }
    }

    measure method "getRows" config (
      exec.benchRuns -> benchRuns
      ) in {
      using(rangeData) in { case (rows, cols, values) =>
        rangeMatrixDouble.getRows(rows)
      }
    }
  }

  performance of "CyclicPartialMatrixDouble" in {
    measure method "update" config (
      exec.benchRuns -> benchRuns
      ) in {
      using(cyclicData) in { case (rows, cols, values) =>
        cyclicMatrixDouble.update(rows, cols, values)
      }
    }

    measure method "get" config (
      exec.benchRuns -> benchRuns
      ) in {
      using(cyclicData) in { case (rows, cols, values) =>
        cyclicMatrixDouble.get(rows, cols)
      }
    }

    measure method "getRows" config (
      exec.benchRuns -> benchRuns
      ) in {
      using(cyclicData) in { case (rows, cols, values) =>
        cyclicMatrixDouble.getRows(rows)
      }
    }
  }

}
