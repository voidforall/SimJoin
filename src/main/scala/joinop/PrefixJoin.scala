package joinop

import math.ceil
import scala.reflect.ClassTag
import org.apache.spark.rdd._
import utils.Distance

/*
 * AllPair Prefix Join
 * AllPair proposed the basic prefix filtering framework:
 *    1. Define a *global* order
 *    2. *Order tokens* based on the global order
 *    3. Select T tokens as *signatures*
 *    4. Check whether there is *overlap* for signatures
 * Ref: Scaling up all pairs similarity search. WWW 2007.
 */
class PrefixJoin(measure: String, threshold: Double, ordering: String, tokenize: String, q: Option[Int]) extends Serializable{

  // Tokenize the String to Array[String]
  // Options: <Char-level> q-gram, <Token-level> Space tokenization
  def tokenize(table: RDD[(Int, String)]): RDD[(Int, Array[String])]={
    tokenize match{
      case "space" =>{
        table.map(x => (x._1, x._2.split(" ")))
      }
      case "qgram" =>{
        if (q.getOrElse(null) == null){
          throw new Exception("Qgram parameter q not set")
        } else{
          table.map(x => (x._1, x._2.sliding(q.get).toArray))
        }
      }
      case _ => throw new Exception("Tokenization scheme not defined")
    }
  }

  // Calculate the prefix threshold given the similarity metric
  // Similarity options: <Char-level> Edit Distance (ED), <Token-level> Jaccard, Cosine, Dice
  def prefixThreshold(length: Int): Int ={
    measure match{
      case "ED" => (threshold * q.get).toInt + 1
      case "Jaccard" => ceil(length * (1-threshold)).toInt + 1
      case "Cosine" => ceil(length * (1-threshold*threshold)).toInt + 1
      case "Dice" => ceil(length * (1-threshold/(2-threshold))).toInt + 1
      case _ => throw new Exception("Measurement not defined")
    }
  }

  // Order the tokens based on the specified global order
  // Ordering options: alphabetical, document frequency (df), inverse document frequency (idf)
  def order(table1: RDD[(Int, Array[String])], table2: Option[RDD[(Int, Array[String])]])
    : (RDD[(Int, Array[String])], Option[RDD[(Int, Array[String])]]) = {
    ordering match{
      case "alphabetical" =>{
        val sorted1: RDD[(Int, Array[String])] = table1.map(x => (x._1, x._2.sorted))
        if(table2.get != null) {
          val sorted2: Option[RDD[(Int, Array[String])]] = Option(table2.get.map(x => (x._1, x._2.sorted)))
          return (sorted1, sorted2)
        } else{
          return (sorted1, null)
        }
      }
      case "df" =>{
        if(table2 != null){
          // Build up frequency decreasing vocabulary
          val tokens: RDD[String] = table1.flatMap(x => x._2).union(table2.get.flatMap(x => x._2))
          val token_freq: List[(String, Long)] = tokens.map(x => (x, 1)).countByKey().toList
          val sorted_tokens: List[String] = token_freq.sortBy(_._2)(Ordering[Long].reverse).map(x => x._1)
          val vocab: Map[String, Int] = sorted_tokens.zipWithIndex.toMap

          // Sort each record according to the global ordering
          val sorted1: RDD[(Int, Array[String])] = table1.map(x => (x._1, x._2.sortBy(token => vocab(token))))
          val sorted2: Option[RDD[(Int, Array[String])]] = Option(table2.get.map(x => (x._1, x._2.sortBy(token => vocab(token)))))

          return (sorted1, sorted2)
        } else{
          val tokens: RDD[String] = table1.flatMap(x => x._2)
          val token_freq: List[(String, Long)] = tokens.map(x => (x, 1)).countByKey().toList
          val sorted_tokens: List[String] = token_freq.sortBy(_._2)(Ordering[Long].reverse).map(x => x._1)
          val vocab: Map[String, Int] = sorted_tokens.zipWithIndex.toMap

          val sorted1: RDD[(Int, Array[String])] = table1.map(x => (x._1, x._2.sortBy(token => vocab(token))))

          return (sorted1, null)
        }
      }
      case "idf" =>{
        ???
      }
      case _ => throw new Exception("Ordering not defined")
    }
  }

  // Generate the prefix for each record, i.e. only select the first T tokens as signatures
  def generatePrefix(table: RDD[(Int, Array[String])]): RDD[(Int, Array[String])] = {
    table.map(x => (x._1, x._2.take(prefixThreshold(x._2.length))))
  }

//  // Verify the candidate pairs and derive exact similarity join results (deprecated)
//  def verify(table: RDD[(String, String)]): RDD[(String, String)] = {
//    val measureObj = new Distance()
//
//    measure match{
//      case "ED" => table.filter(x => measureObj.editDistance(x._1.toString, x._2.toString) <= threshold)
//      case "Jaccard" => table.filter(x => measureObj.jaccard(Set(x._1), Set(x._2)) <= threshold)
//      case "Cosine" =>table.filter(x => measureObj.cosine(Set(x._1), Set(x._2)) <= threshold)
//      case "Dice" =>table.filter(x => measureObj.dice(Set(x._1), Set(x._2)) <= threshold)
//      case _ => throw new Exception("Measurement not defined")
//    }
//  }

  // Similarity self join, with distinct keys by default
  def selfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
    val tableMap = table.collectAsMap() // TODO: avoid collect
    val tokenized: RDD[(Int, Array[String])] = tokenize(table)
    val tokenizedMap = tokenized.collectAsMap()

    val ordered: RDD[(Int, Array[String])] = order(tokenized, null)._1
    val prefixed: RDD[(Int, Array[String])] = generatePrefix(ordered)

    // Build up the inverted index
    val id_token: RDD[(Int, String)] = prefixed.flatMapValues(x => x)
    val inverted_index: RDD[(String, List[Int])] = id_token
      .map({case(a, b) => (b, a)})
      .groupByKey
      .mapValues(_.toList)

    // Generate potential candidate pairs
    val potential_candidates: RDD[(String, List[List[Int]])] = inverted_index.map(pair=>(pair._1,pair._2.combinations(2).toList))
    val candidates: RDD[(Int, Int)] = potential_candidates
      .values.map(x => x.map(y => y match {
        case List(a,b) => (a,b)}))
      .flatMap(x => x)
      .filter(x => (x._1 != x._2))
      .distinct

    println("[AllPairs] There are " + candidates.count.toString + " candidate pairs after prefix pruning.")

    // Verification step
    val measureObj = new Distance()
    val verified_pairs: RDD[(Int, Int)] = measure match{
      case "ED" =>
        candidates.filter(x => measureObj.editDistance(tableMap(x._1), tableMap(x._2)) <= threshold)
      case "Jaccard" =>
        candidates.filter(x => measureObj.jaccard(tokenizedMap(x._1).toSet, tokenizedMap(x._2).toSet) <= threshold)
      case "Cosine" =>
        candidates.filter(x => measureObj.cosine(tokenizedMap(x._1).toSet, tokenizedMap(x._2).toSet) <= threshold)
      case "Dice" =>
        candidates.filter(x => measureObj.dice(tokenizedMap(x._1).toSet, tokenizedMap(x._2).toSet) <= threshold)
      case _ => throw new Exception("Measurement not defined")
    }

    // Format the output
    val output_pairs: RDD[((Int, String), (Int, String))] = verified_pairs
      .map(x => (if (x._1 < x._2) x else (x._2, x._1)))
      .sortByKey()
      .map(x => ((x._1, tableMap(x._1)), (x._2, tableMap(x._2))))

    output_pairs
  }

  // Similarity RS join
  def rsJoin(table1: RDD[(Int, String)], table2: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
    val tokenized1: RDD[(Int, Array[String])] = tokenize(table1)
    val tokenized2: RDD[(Int, Array[String])] = tokenize(table2)

    val (ordered1, ordered2) = order(tokenized1, Option(tokenized2))
    val prefixed1: RDD[(Int, Array[String])] = generatePrefix(ordered1)
    val prefixed2: RDD[(Int, Array[String])] = generatePrefix(ordered2.get)

    // Build up the inverted index
    val id_token1: RDD[(Int, String)] = prefixed1.flatMapValues(x => x)
    val id_token2: RDD[(Int, String)] = prefixed2.flatMapValues(x => x)
    val inverted_index1: RDD[(String, List[Int])] = id_token1
      .map({case(a, b) => (b, a)})
      .groupByKey
      .mapValues(_.toList)
    val inverted_index2: RDD[(String, List[Int])] = id_token2
      .map({case(a, b) => (b, a)})
      .groupByKey
      .mapValues(_.toList)
    val inverted_index: RDD[(String, (List[Int], List[Int]))] = inverted_index1.join(inverted_index2)

    // Generate potential candidate pairs
    val potential_candidates: RDD[(String, List[List[Int]])] =
      inverted_index.map({case(k, (l1, l2)) =>
        (k, l1.flatMap(x => l2.map(y => List(x, y))))})
    val candidates: RDD[(Int, Int)] = potential_candidates
      .values.map(x => x.map(y => y match {
      case List(a,b) => (a,b)}))
      .flatMap(x => x)
      .distinct

    // Verification step
    val measureObj = new Distance()
    val verified_pairs: RDD[(Int, Int)] = measure match{
      case "ED" =>
        candidates.filter(x => measureObj.editDistance(table1.lookup(x._1)(0), table2.lookup(x._2)(0)) <= threshold)
      case "Jaccard" =>
        candidates.filter(x => measureObj.jaccard(tokenized1.lookup(x._1)(0).toSet, tokenized2.lookup(x._2)(0).toSet) <= threshold)
      case "Cosine" =>
        candidates.filter(x => measureObj.cosine(tokenized1.lookup(x._1)(0).toSet, tokenized2.lookup(x._2)(0).toSet) <= threshold)
      case "Dice" =>
        candidates.filter(x => measureObj.dice(tokenized1.lookup(x._1)(0).toSet, tokenized2.lookup(x._2)(0).toSet) <= threshold)
      case _ => throw new Exception("Measurement not defined")
    }

    // Format the output
    val output_pairs: RDD[((Int, String), (Int, String))] = verified_pairs
      .map(x => (if (x._1 < x._2) x else (x._2, x._1)))
      .sortByKey()
      .map(x => ((x._1, table1.lookup(x._1)(0)), (x._2, table2.lookup(x._2)(0))))

    output_pairs
  }

}
