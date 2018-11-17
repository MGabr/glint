package glint.iterators

import akka.util.Timeout
import glint.SystemTest
import glint.mocking.MockBigMatrix
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * RetryBigMatrix test specification
  */
class RowIteratorSpec extends FlatSpec with SystemTest with Matchers {

  "A RowIterator" should "iterate over all rows in order" in {

    // Construct mock matrix and data to push into it
    val nrOfRows = 5
    val nrOfCols = 2
    val mockMatrix = new MockBigMatrix[Long](nrOfRows, nrOfCols, 0, _ + _)

    val rows   = Array(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L)
    val cols   = Array(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L)
    val values = Array(0L,  1,  2,  3,  4,  5,  6,  7,  8,  9)

    whenReady(mockMatrix.push(rows, cols, values)) { identity }

    // Check whether elements are in order
    var counter = 0
    val iterator = new RowIterator[Long](mockMatrix, 2)
    iterator.foreach {
      case row => row.foreach {
        case value =>
          assert(value == counter)
          counter += 1
      }
    }

  }

  it should "iterate over a single block" in {

    // Construct mock matrix and data to push into it
    val nrOfRows = 5
    val nrOfCols = 2
    val mockMatrix = new MockBigMatrix[Long](nrOfRows, nrOfCols, 0, _ + _)

    val rows = Array(0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L)
    val cols = Array(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L)
    val values = Array(0L, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    whenReady(mockMatrix.push(rows, cols, values)) {
      identity
    }

    // Check whether elements are in order
    var counter = 0
    val iterator = new RowIterator[Long](mockMatrix, 7)
    iterator.foreach {
      case row => row.foreach {
        case value =>
          assert(value == counter)
          counter += 1
      }
    }
    assert(!iterator.hasNext)

  }

  it should "not iterate over an empty matrix" in {
    val mockMatrix = new MockBigMatrix[Double](3, 0, 0, _ + _)

    val iterator = new RowBlockIterator[Double](mockMatrix, 3)
    assert(!iterator.hasNext)
    iterator.foreach {
      case _ => fail("This should never execute")
    }

  }

}
