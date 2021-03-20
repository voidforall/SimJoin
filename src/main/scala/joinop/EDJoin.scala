package joinop

import org.apache.spark.rdd.RDD
import utils.Distance
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class EDJoin(measure: String, threshold: Double, ordering: String, tokenize: String, q: Int) extends Serializable{
  protected val measureObj = new Distance()

  // Tokenize the String to generate positional q-gram
  // e.g. "science" -> Array[(sci, 0), (cie, 1), ... (nce, 4)]
  // For q-grams, tokenizer adds q-1 # prefix and q-1 $suffix
  def tokenize(table: RDD[(Int, String)]): RDD[(Int, String, Array[(String, Int)])] ={
    tokenize match{
      case "qgram" =>
        if (q <= 0){
          throw new Exception("Qgram parameter q not set")
        } else{
          table.map(x => (x._1, "#"*(q-1) + x._2 + "$"*(q-1),
            ("#"*(q-1) + x._2 + "$"*(q-1)).sliding(q).zipWithIndex.map(q => (q._1, q._2)).toArray))
        }
      case _ => throw new Exception("Tokenization scheme not defined")
    }
  }

  // Order the tokens based on the specified global order
  // EDJoin: sort with decreasing order of idf and increasing order of locations
  // decreasing order of idf means that the rarest q-gram will be the head
  def order(table1: RDD[(Int, String, Array[(String, Int)])], table2: Option[RDD[(Int, String, Array[(String, Int)])]])
    : (RDD[(Int, String, Array[(Int, Int)])], Option[RDD[(Int, String, Array[(Int, Int)])]]) = {
    ordering match{
      case "idf" =>
        if(table2 != null){
          val vocab: Map[String, Int] = table1.flatMap{
            case(id, str, qgrams) => qgrams.map{case (qgram, loc) => qgram}
          }
            .union(
              table2.get.flatMap{
                case(id, str, qgrams) => qgrams.map{case (qgram, loc) => qgram}
              }
            )
            .groupBy(x => x)
            .map(x => (x._1, x._2.size))
            .collectAsMap().toMap

          val vocab_id = table1.context.broadcast(vocab.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)

          (table1.map{ case(id, str, qgrams) => (id, str, qgrams.map(x => (vocab_id.value(x._1), x._2)).sortBy(x => x))}
            , Option(table2.get.map{ case(id, str, qgrams) => (id, str, qgrams.map(x => (vocab_id.value(x._1), x._2)).sortBy(x => x))}))

        } else{
          val vocab: Map[String, Int] = table1.flatMap{
            case(id, str, qgrams) => qgrams.map{case (qgram, loc) => qgram}
          }
            .groupBy(x => x)
            .map(x => (x._1, x._2.size))
            .collectAsMap().toMap
          val vocab_id = table1.context.broadcast(vocab.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)

          (table1.map{ case(id, str, qgrams) => (id, str, qgrams.map(x => (vocab_id.value(x._1), x._2)).sortBy(x => x))}, null)
        }
      case _ => throw new Exception("Ordering not defined")
    }
  }

  def minEditErrors(qgrams: Array[(Int, Int)]): Int = {
    var cnt: Int = 0
    var loc: Int = 0
    for (i <- qgrams.indices){
      if(qgrams(i)._2 > loc){
        cnt += 1
        loc = qgrams(i)._2 + q - 1
      }
    }
    cnt
  }

  // Calculate minimum prefix length, according to location-based mismatch filtering in EDJoin
  // Ref: Algorithm 2, 3 in Section 3 "Location-based mismatch filtering and ED-Join"
  def calcPrefixLen(qgrams: Array[(Int, Int)]): Int = {
    // Binary search within the range
    var left: Int = (threshold + 1).toInt
    var right: Int = (q * threshold + 1).toInt
    var mid: Int = 0
    var err: Int = 0
    while (left < right){
      mid = (left +  right) / 2
      err = minEditErrors(qgrams.take(mid))
      if (err < threshold)
        left = mid + 1
      else
        right = mid
    }
    left
  }

  // Generate the prefix for each record, i.e. only select the first T tokens as signatures
  // and then build up the inverted index as (qgram, Array[instance])
  // p.s. EDJoin applies location-based filtering to select minimum prefix length, rather than (q * threshold + 1)
  def buildIndex(table: RDD[(Int, String, Array[(Int, Int)])]): RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])] = {
    val tokens_map = table.flatMap{
      case (id, str, qgrams) =>
//        val prefixLen: Int = calcPrefixLen(qgrams) // TODO: debug overpruning
        val prefixLen: Int = (threshold * q).toInt + 1
        val prefix: Array[(Int, Int)] = qgrams.take(prefixLen)
        prefix.zipWithIndex.map{ case (qgram, index) =>
          (qgram._1, (id, index, qgrams, str))
        }
    }
    tokens_map.groupByKey()
      .filter(_._2.size > 1) // filter out tokens with only one entry
      .map(x => (x._1, x._2.toArray))
  }

  // Compare two sorted array of q-grams, and derive the mismatching q-grams
  // also used for count filtering
  // Ref: Algorithm 8 in the paper
  def compareQGrams(pairs: (Array[(Int, Int)], Array[(Int, Int)])): (Array[(Int, Int)], Int)={
    var count: Int = 0 // mismatched q-grams
    var i = 0; var j = 0
    val Q: ArrayBuffer[(Int, Int)] = ArrayBuffer[(Int, Int)]() // loosely mismatching q-grams from x to y

    while (i < pairs._1.length && j < pairs._2.length){
      if (pairs._1(i)._1 == pairs._2(j)._1){
        if (math.abs(pairs._1(i)._2 - pairs._2(j)._2) <= threshold){
          i += 1; j += 1
        }
        else{
          if (pairs._1(i)._2 < pairs._2(j)._2){ // x[i].loc < y[j].loc
            if((i != 0 && pairs._1(i)._1 != pairs._1(i-1)._1)
              || (j != 0 && pairs._1(i)._1 != pairs._2(j-1)._1)
              || (j != 0 && math.abs(pairs._1(i)._2 - pairs._2(j-1)._2) > threshold)){
              Q += pairs._1(i)
            }
            count += 1; i += 1
          }
          else{
            j += 1
          }
        }
      }
      else{
        if (pairs._1(i)._1 < pairs._2(j)._1){
          if((i != 0 && pairs._1(i)._1 != pairs._1(i-1)._1)
            || (j != 0 && pairs._1(i)._1 != pairs._2(j-1)._1)
            || (j != 0 && math.abs(pairs._1(i)._2 - pairs._2(j-1)._2) > threshold)){
            Q += pairs._1(i)
          }
          count += 1; i += 1
        }
        else{
          j += 1
        }
      }
    }

    while (i < pairs._1.length){
      if((i != 0 && pairs._1(i)._1 != pairs._1(i-1)._1)
        || (j != 0 && pairs._1(i)._1 != pairs._2(j-1)._1)
        || (j != 0 && math.abs(pairs._1(i)._2 - pairs._2(j-1)._2) > threshold)){
        Q += pairs._1(i)
      }
      count += 1; i += 1
    }
    (Q.toArray, count)
  }

  // Apply content filtering and derive a lower bound of ed(s, t)
  // Ref: Algorithm 5 in the paper
  def contentFilter(s: String, t: String, Q: Array[(Int, Int)]): Int ={
    def L1Distance(s: String, t: String, lo: Int, hi: Int): Int ={
      var L1 = 0
      val freqMap = scala.collection.mutable.Map[Char, Int]()

      for(i <- lo to hi){
        freqMap.get(s(i)) match{
          case Some(e) => freqMap.update(s(i), e + 1)
          case None => freqMap.put(s(i), 1)
        }
      }
      for(i <- lo to hi if i < t.length){
        freqMap.get(t(i)) match{
          case Some(e) => freqMap.update(s(i), e - 1)
          case None => freqMap.put(s(i), -1)
        }
      }
      for(v <- freqMap.values){
        L1 += Math.abs(v)
      }
      L1
    }

    def SumRightErrs(loc: Int, suffixList: Iterator[(Int, Int)]): Int ={
      while(suffixList.hasNext){
        val entry = suffixList.next()
        if(entry._1 >= loc)
          return entry._2
      }
      0
    }

    if(Q.length == 0) return 0

    // Build up a condensed suffix sum list of Q (loc, errors)
    var cnt = 1
    var loc = Q(Q.length - 1)._2 + 1
    val suffixSumList: ListBuffer[(Int, Int)] = new ListBuffer()
    for (i <- (0 to Q.length - 1).reverse){
      if (Q(i)._2 <= loc){
        suffixSumList.prepend((Q(i)._2, cnt+1))
        cnt += 1
        loc = Q(i)._2 - q
      }
    }

    var i = 1; var j = 0
    var bound = 0
    val iter = suffixSumList.toList.iterator
    while (i < Q.length){
      if (Q(i)._2 - Q(i-1)._2 > 1){
        bound = L1Distance(s, t, Q(j)._2, Q(i-1)._2 + q - 1) + SumRightErrs(Q(i-1)._2 + q, iter)
        if (bound > 2 * threshold){
          return (2 * threshold + 1).toInt
        }
        j = i
      }
      i += 1
    }
    L1Distance(s, t, Q(j)._2, Q(i-1)._2 + q - 1) + SumRightErrs(Q(i-1)._2 + q, iter)
  }

  // Verify the candidate pairs and derive exact similarity join results
  // EDJoin integrates three filtering methods before running edit distance calculation
  // i.e., count and position filtering, location-based mismatch filtering, and content-based mis-match filtering
  // Ref: Algorithm 7 (Verify) in the paper
  def verify(candidate: ((Int, Array[(Int, Int)], String), (Int, Array[(Int, Int)], String))): Boolean = {
    val (mismatchedQ, count) = compareQGrams(candidate._1._2, candidate._2._2)
    var valid: Boolean = false
    if (count <= q * threshold){ // count filtering
      val locCount = minEditErrors(mismatchedQ)
      if (locCount <= threshold){ // location-based mismatch filtering
        val bound: Int = contentFilter(candidate._1._3, candidate._2._3, mismatchedQ)
        if(bound <= 2 * threshold){ // content filtering
          if (measureObj.editDistance(candidate._1._3, candidate._2._3) <= threshold) // last option: compute edit distance
            valid = true
        }
      }
    }
    valid
  }

  def selfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
    val tokenized: RDD[(Int, String, Array[(String, Int)])] = tokenize(table)
    val ordered: RDD[(Int, String, Array[(Int, Int)])] = order(tokenized, null)._1
    val invertedIndex: RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])] = buildIndex(ordered)

    // Generate potential candidate pairs
    val potential_candidates: RDD[(Int, Array[Array[(Int, Int, Array[(Int, Int)], String)]])] =
      invertedIndex.map(pair => (pair._1, pair._2.combinations(2).toArray))
    val candidates: RDD[((Int, Array[(Int, Int)], String), (Int, Array[(Int, Int)], String))] = potential_candidates.
      values.map(x => x.map {
      case Array(a, b) => (a, b)
    })
      .flatMap(x => x)
      .filter(x => x._1._1 != x._2._1  && // not the same id
//        math.abs(x._1._2 - x._2._2) <= threshold && // positional filtering // TODO: debug overpruning
        math.abs(x._1._3.length - x._2._3.length) <= threshold // length filtering
      )
      .map(x => ((x._1._1, x._1._3, x._1._4), (x._2._1, x._2._3, x._2._4)))
      .distinct

    // Verification step in the framework
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
