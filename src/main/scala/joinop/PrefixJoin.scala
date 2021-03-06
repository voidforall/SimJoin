package joinop

import math.ceil
import java.util.Calendar
import org.apache.spark.rdd._
import utils.Distance

import java.io.{BufferedWriter, FileWriter}

/*
 * The class implements the basic prefix filtering framework:
 *    1. Define a *global* order
 *    2. *Order tokens* based on the global order
 *    3. Select T tokens as *signatures*
 *    4. Check whether there is *overlap* for signatures
 *
 * This implementation serves as the baseline and is open to
 * complement with further optimizations like positional filtering.
 */

class PrefixJoin(measure: String, threshold: Double, ordering: String, tokenize: String, q: Int, stepReport: Boolean, detailReport: Boolean) extends Serializable{
  var measureObj = new Distance()

  // Tokenize the String to Array[String]
  // Options: <Char-level> q-gram, <Token-level> Space tokenization
  // For q-grams, tokenizer adds q-1 # prefix and q-1 $suffix
  def tokenize(table: RDD[(Int, String)]): RDD[(Int, String, Array[String])]={
    tokenize match{
      case "space" =>
        table.map(x => (x._1, x._2, x._2.split(" ")))
      case "qgram" =>
          table.map(x => (x._1, "#"*(q-1) + x._2 + "$"*(q-1), ("#"*(q-1) + x._2 + "$"*(q-1)).sliding(q).toArray))
      case _ => throw new Exception("Tokenization scheme not defined")
    }
  }

  // Calculate the prefix threshold given the similarity metric
  // Similarity options: <Char-level> Edit Distance (ED), <Token-level> Jaccard, Cosine, Dice
  def prefixThreshold(length: Int): Int ={
    measure match{
      case "ED" => (threshold * q).toInt + 1
      case "Jaccard" => ceil(length * threshold).toInt + 1
      case "Cosine" => ceil(length * (1-threshold*threshold)).toInt + 1
      case "Dice" => ceil(length * (1-threshold/(2-threshold))).toInt + 1
      case _ => throw new Exception("Measurement not defined")
    }
  }

  // Order the tokens based on the specified global order
  // Ordering options: alphabetical, inverse document frequency (idf)
  def order(table1: RDD[(Int, String, Array[String])], table2: Option[RDD[(Int, String, Array[String])]])
    : (RDD[(Int, String, Array[Int])], Option[RDD[(Int, String, Array[Int])]]) = {
    ordering match{
      case "alphabetical" =>
        val sorted1: RDD[(Int, String, Array[Int])] = table1.map(x => (x._1, x._2, x._3.sorted.map(x => x.toInt)))
        if(table2 != null) {
          val sorted2: Option[RDD[(Int, String, Array[Int])]] = Option(table2.get.map(x => (x._1, x._2, x._3.sorted.map(x => x.toInt))))
          (sorted1, sorted2)
        } else{
          (sorted1, null)
        }
      case "idf" =>
        if(table2 != null){
          val vocab: Map[String, Int] = table1.flatMap{
            case(id, str, qgrams) => qgrams
          }
            .union(
              table2.get.flatMap{
                case(id, str, qgrams) => qgrams
              }
            )
            .groupBy(x => x)
            .map(x => (x._1, x._2.size))
            .collectAsMap().toMap

          val vocab_id = table1.context.broadcast(vocab.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)

          (table1.map{ case(id, str, qgrams) => (id, str, qgrams.map(x => vocab_id.value(x)).sortBy(x => x))}
            , Option(table2.get.map{ case(id, str, qgrams) => (id, str, qgrams.map(x => vocab_id.value(x)).sortBy(x => x))}))

        } else{
          val vocab: Map[String, Int] = table1.flatMap{
            case(id, str, qgrams) => qgrams
          }
            .groupBy(x => x)
            .map(x => (x._1, x._2.size))
            .collectAsMap().toMap
          val vocab_id = table1.context.broadcast(vocab.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)

          (table1.map{ case(id, str, qgrams) => (id, str, qgrams.map(x => vocab_id.value(x)).sortBy(x => x))}, null)
        }
      case _ => throw new Exception("Ordering not defined")
    }
  }

  // Generate the prefix for each record, i.e. only select the first T tokens as signatures
  def generatePrefix(table: RDD[(Int, String, Array[Int])]): RDD[(Int, String, Array[Int])] = {
    table.map(x => (x._1, x._2, x._3.take(prefixThreshold(x._3.length))))
  }

  // Verify the candidate pairs and derive exact similarity join results
  def verify(candidates: RDD[((Int, String), (Int, String))]): RDD[((Int, String), (Int, String))] = {
    measure match{
      case "ED" => candidates.filter(x => measureObj.editDistance(x._1._2, x._2._2) <= threshold)
      case "Jaccard" => candidates.filter(x => measureObj.jaccard(x._1._2.split(" ").toSet, x._2._2.split(" ").toSet) <= threshold)
      case "Cosine" => candidates.filter(x => measureObj.cosine(x._1._2.toSet, x._2._2.toSet) <= threshold)
      case "Dice" => candidates.filter(x => measureObj.dice(x._1._2.toSet, x._2._2.toSet) <= threshold)
      case _ => throw new Exception("Measurement not defined")
    }
  }

  // Similarity self join, with distinct keys by default
  def selfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {

    val ts = Calendar.getInstance().getTimeInMillis
    val getTime = Calendar.getInstance().getTimeInMillis

    // Tokenization as q-grams (character-level)
    val tokenized: RDD[(Int, String, Array[String])] = tokenize(table)
    if (stepReport) {
      println(tokenized.count)
      println("[Tokenize] Elapsed time: " + (getTime - ts) / 1000.0 + "s")
    }

    // Ordering with specified global order
    val ordered: RDD[(Int, String, Array[Int])] = order(tokenized, null)._1
    if (stepReport) {
      println(ordered.count)
      println("[Order] Elapsed time: " + (getTime - ts) / 1000.0 + "s")
    }

    // Build up the inverted index
    val prefixed: RDD[(Int, String, Array[Int])] = generatePrefix(ordered)
    val id_token: RDD[((Int, String), Int)] = prefixed.map(x => ((x._1, x._2), x._3)).flatMapValues(x => x)
    val inverted_index: RDD[(Int, List[(Int, String)])] = id_token
      .map({case(a, b) => (b, a)})
      .groupByKey
      .mapValues(_.toList)
    if (stepReport) {
      println("[Buildindex] Number of token entries: " + inverted_index.count)
      println("[Buildindex] Number of useful token entries: " + inverted_index.filter(x => x._2.length > 1).count)
      println("[Buildindex] Elapsed time: " + (getTime - ts) / 1000.0 + "s")
    }
    if (detailReport){
      println("[Buildindex] Number of useful token entries: " + inverted_index.filter(x => x._2.length > 1).count)
      val prefix_list = inverted_index.filter(x => x._2.length > 1).map(x => x._2.length).collect()
      val prefix_file = "/scratch/yuan/data/log_data/prefix.txt"
      val prefix_writer = new BufferedWriter(new FileWriter(prefix_file))
      for (i <- prefix_list)
        prefix_writer.write(i.toString + "\n")
      prefix_writer.close()
    }

    // Generate potential candidate pairs
    val potential_candidates: RDD[(Int, List[List[(Int, String)]])] = inverted_index.map(pair => (pair._1, pair._2.combinations(2).toList))
    val candidates: RDD[((Int, String), (Int, String))] = potential_candidates
      .values.map(x => x.map {
        case List(a, b) => (a, b)
      })
      .flatMap(x => x)
      .filter(x => x._1 != x._2)
      .distinct
    if (stepReport) {
      println("[Candidates] Number of distinct potential pairs: " +
        candidates.map(x => if (x._1._1 < x._2._1) (x._1._1, x._2._1) else (x._2._1, x._1._1)).distinct.count)
      println("[Candidates] Elapsed time: " + (getTime - ts) / 1000.0 + "s")
    }
    if (detailReport){
      println("[Candidates] Number of distinct potential pairs: " +
        candidates.map(x => if (x._1._1 < x._2._1) (x._1._1, x._2._1) else (x._2._1, x._1._1)).distinct.count)
    }

    // Verification step
    val verified_pairs: RDD[((Int, String), (Int, String))] = verify(candidates)
    if (stepReport) {
        println(verified_pairs.count)
        println("[Verification] Elapsed time: " + (getTime - ts) / 1000.0 + "s")
    }

    // Format the output
    val output_pairs: RDD[((Int, String), (Int, String))] = verified_pairs
      .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
      .sortByKey()

    output_pairs
  }

  // Similarity RS join
  def rsJoin(table1: RDD[(Int, String)], table2: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
    val tokenized1: RDD[(Int, String, Array[String])] = tokenize(table1)
    val tokenized2: RDD[(Int, String, Array[String])] = tokenize(table2)

    val (ordered1, ordered2) = order(tokenized1, Option(tokenized2))
    val prefixed1: RDD[(Int, String, Array[Int])] = generatePrefix(ordered1)
    val prefixed2: RDD[(Int, String, Array[Int])] = generatePrefix(ordered2.get)

    // Build up the inverted index
    val id_token1: RDD[((Int, String), Int)] = prefixed1.map(x => ((x._1, x._2), x._3)).flatMapValues(x => x)
    val id_token2: RDD[((Int, String), Int)] = prefixed2.map(x => ((x._1, x._2), x._3)).flatMapValues(x => x)
    val inverted_index1: RDD[(Int, List[(Int, String)])] = id_token1
      .map({case(a, b) => (b, a)})
      .groupByKey
      .mapValues(_.toList)
    val inverted_index2: RDD[(Int, List[(Int, String)])] = id_token2
      .map({case(a, b) => (b, a)})
      .groupByKey
      .mapValues(_.toList)
    val inverted_index: RDD[(Int, (List[(Int, String)], List[(Int, String)]))] = inverted_index1.join(inverted_index2)

    // Generate potential candidate pairs
    val potential_candidates: RDD[(Int, List[List[(Int, String)]])] =
      inverted_index.map({case(k, (l1, l2)) =>
        (k, l1.flatMap(x => l2.map(y => List(x, y))))})
    val candidates: RDD[((Int, String), (Int, String))] = potential_candidates
      .values.map(x => x.map {
        case List(a, b) => (a, b)
      })
      .flatMap(x => x)
      .distinct

    // Verification step
    val verified_pairs: RDD[((Int, String), (Int, String))] = verify(candidates)

    // Format the output
    val output_pairs: RDD[((Int, String), (Int, String))] = verified_pairs
      .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
      .sortByKey()

    output_pairs
  }
}
