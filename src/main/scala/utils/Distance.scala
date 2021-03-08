package utils

import scala.math.sqrt

class Distance extends Serializable{
  def editDistance(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) {
      (j, i) => if (j == 0) i else if (i == 0) j else 0
    }

    @inline
    def minimum(i: Int*): Int = i.min

    for {j <- dist.indices.tail
         i <- dist(0).indices.tail} dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }

  def jaccard(s1: Set[Any], s2: Set[Any]): Double = {
    s1.intersect(s2).size.toDouble / s1.union(s2).size.toDouble
  }

  def cosine(s1: Set[Any], s2: Set[Any]): Double = {
    s1.intersect(s2).size.toDouble / sqrt(s1.size.toDouble * s2.size.toDouble)
  }

  def dice(s1: Set[Any], s2: Set[Any]): Double = {
    2 * s1.intersect(s2).size.toDouble / (s1.size.toDouble + s2.size.toDouble)
  }
}
