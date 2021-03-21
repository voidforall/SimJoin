package joinop

import org.apache.spark.rdd.RDD
import utils.Distance

/*
 * GramCount Join
 * Similarity join based on q-gram based count filtering.
 * The techniques include count filtering, position filtering, and length filtering.
 *
 * Ref: Approximate String Joins in a Database (Almost) for Free (VLDB01')
 *
 */
class GramCountJoin(threshold: Int, q: Int) extends Serializable {
  protected val measureObj = new Distance()

  // Tokenize the String to positional q-gram
  def tokenize(table: RDD[(Int, String)]): RDD[(Int, String, Array[(String, Int)])] = {
    table.map(x => (x._1, "#"*(q-1) + x._2 + "$"*(q-1),
      ("#"*(q-1) + x._2 + "$"*(q-1)).sliding(q).zipWithIndex.map(q => (q._1, q._2)).toArray))
  }

  // Generate inverted index for the records, with q-gram as group key
  def buildIndex(table: RDD[(Int, String, Array[(String, Int)])]): RDD[(String, Array[(Int, Int, Array[(String, Int)], String)])] = {
    val tokens_map = table.flatMap{
      case (id, str, qgrams) =>
        qgrams.zipWithIndex.map{ case (qgram, index) =>
          (qgram._1, (id, qgram._2, qgrams, str)) // Note: the q-grams have been ordered and we need to use original position rather than index
        }
    }
    tokens_map.groupByKey()
      .filter(_._2.size > 1) // filter out tokens with only one entry value
      .map(x => (x._1, x._2.toArray))
  }

  // Verify the candidate pairs and derive exact similarity join results
  // GramCount only applies *count filtering* in verify step
  def verify(candidate: ((Int, Array[(String, Int)], String), (Int, Array[(String, Int)], String))): Boolean = {
    var valid: Boolean = false
    val count: Int = List(candidate._1._2.map(_._1), candidate._2._2.map(_._1)).reduce((a, b) => a intersect b).length
//    val count_threshold: Int = math.max(candidate._1._2.length, candidate._2._2.length) - 1 - (threshold - 1) * q
    val count_threshold: Int = math.max(candidate._1._2.length, candidate._2._2.length) - q + 1 - threshold * q
    if (count >= count_threshold) { // count filtering
      if (measureObj.editDistance(candidate._1._3, candidate._2._3) <= threshold) // last option: compute edit distance
        valid = true
    }
    valid
  }

  // Similarity self join, with distinct keys by default
  def selfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
    val tokenized: RDD[(Int, String, Array[(String, Int)])] = tokenize(table)
    val invertedIndex: RDD[(String, Array[(Int, Int, Array[(String, Int)], String)])] = buildIndex(tokenized)

    // Generate potential candidate pairs
    val potential_candidates: RDD[(String, Array[Array[(Int, Int, Array[(String, Int)], String)]])] =
      invertedIndex.map(pair => (pair._1, pair._2.combinations(2).toArray))
    val candidates: RDD[((Int, Array[(String, Int)], String), (Int, Array[(String, Int)], String))] = potential_candidates.
      values.map(x => x.map {
      case Array(a, b) => (a, b)
    })
      .flatMap(x => x)
      .filter(x => x._1._1 != x._2._1  && // not the same id
        math.abs(x._1._2 - x._2._2) <= threshold && // positional filtering
        math.abs(x._1._3.length - x._2._3.length) <= threshold // length filtering
      )
      .map(x => ((x._1._1, x._1._3, x._1._4), (x._2._1, x._2._3, x._2._4)))
      .distinct

    // Verification step
    val verified_pairs: RDD[((Int, String), (Int, String))] = candidates.filter(x => verify(x._1, x._2))
      .map(x => ((x._1._1, x._1._3), (x._2._1, x._2._3)))

    // Format the output
    val output_pairs: RDD[((Int, String), (Int, String))] = verified_pairs
      .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
      .sortByKey()
      .distinct

    output_pairs
  }
}