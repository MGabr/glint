package glint.spark

import glint.Client
import org.scalatest.FlatSpec

class ClientSpec extends FlatSpec with SparkTest {

  "A client" should "run on Spark" taggedAs SparkTest in withContext { sc =>
    val client = Client.runOnSpark(sc)()
    client.terminateOnSpark(sc)
  }
}
