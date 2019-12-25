package glint.matrix

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import glint.models.server.PartialMatrixDouble
import glint.partitioning.by.PartitionBy
import glint.partitioning.range.RangePartition
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
  val rangePartition = RangePartition(0, 0, 10000, PartitionBy.ROW) // 10000 elements

  // Construct matrices for range and cyclic partitions
  val rangeMatrixDoubleRef = TestActorRef(new PartialMatrixDouble(rangePartition.index, 10000, 300, None, None))
  val rangeMatrixDouble = rangeMatrixDoubleRef.underlyingActor

  // Sizes and data
  val sizes = Gen.range("size")(4000, 10000, 2000)
  val rangeData = for (size <- sizes) yield {
    val rows = (0 until size).map { case x => x }.toArray
    val cols = (0 until size).map { case x => x % 300 }.toArray
    val values = (0 until size).map(_ => random.nextDouble()).toArray
    (rows, cols, values)
  }

  // Number of benchmark runs
  val benchRuns = 60

  performance of "PartialMatrixDouble" in {
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
}
