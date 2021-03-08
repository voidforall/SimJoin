package utils

import org.scalatest.FunSuite
import utils.Distance
import scala.math.sqrt

class DistanceTest extends FunSuite{
  val measureObj = new Distance()
  test("Distance.editDistance"){
    assert(measureObj.editDistance("EPFL", "EPFL") == 0)
    assert(measureObj.editDistance("Lausane", "Lausanne") == 1)
    assert(measureObj.editDistance("kitten", "sitting") == 3)
  }

  val r: Set[Any] = Set("frontier", "computer", "science")
  val s: Set[Any] = Set("computer", "science")
  test("Distance.setDistance"){
    assert(measureObj.jaccard(r, s) == (2.0/3.0))
    assert(measureObj.cosine(r, s) == (2.0/sqrt(6)))
    assert(measureObj.dice(r, s) == (4.0/5.0))
  }
}
