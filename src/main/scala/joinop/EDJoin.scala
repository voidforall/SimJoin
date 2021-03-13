package joinop

import org.apache.spark.rdd.RDD
import utils.Distance

class EDJoin protected (measure: String, threshold: Double, ordering: String, tokenize: String, q: Option[Int]
                       ) extends PrefixJoin(measure, threshold, ordering, tokenize, q){


//  // Similarity self join, with distinct keys by default
//  override def selfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
//    val tableMap = table.collectAsMap() // TODO: avoid collect
//    val tokenized: RDD[(Int, Array[String])] = tokenize(table)
//    val tokenizedMap = tokenized.collectAsMap()
//
//    val ordered: RDD[(Int, Array[String])] = order(tokenized, null)._1
//    val prefixed: RDD[(Int, Array[String])] = generatePrefix(ordered)
//
//    // Build up the inverted index
//    val id_token: RDD[(Int, String)] = prefixed.flatMapValues(x => x)
//    val inverted_index: RDD[(String, List[Int])] = id_token
//      .map({case(a, b) => (b, a)})
//      .groupByKey
//      .mapValues(_.toList)
//
//    // Generate potential candidate pairs
//    val potential_candidates: RDD[(String, List[List[Int]])] = inverted_index.map(pair=>(pair._1,pair._2.combinations(2).toList))
//    val candidates: RDD[(Int, Int)] = potential_candidates
//      .values.map(x => x.map(y => y match {
//      case List(a,b) => (a,b)}))
//      .flatMap(x => x)
//      .filter(x => (x._1 != x._2))
//      .distinct
//
//    println("[EDJoin] There are " + candidates.count.toString + " candidate pairs after prefix pruning.")
//
//    // Verification step
//    val measureObj = new Distance()
//    val verified_pairs: RDD[(Int, Int)] = measure match{
//      case "ED" =>
//        candidates.filter(x => measureObj.editDistance(tableMap(x._1), tableMap(x._2)) <= threshold)
//      case "Jaccard" =>
//        candidates.filter(x => measureObj.jaccard(tokenizedMap(x._1).toSet, tokenizedMap(x._2).toSet) <= threshold)
//      case "Cosine" =>
//        candidates.filter(x => measureObj.cosine(tokenizedMap(x._1).toSet, tokenizedMap(x._2).toSet) <= threshold)
//      case "Dice" =>
//        candidates.filter(x => measureObj.dice(tokenizedMap(x._1).toSet, tokenizedMap(x._2).toSet) <= threshold)
//      case _ => throw new Exception("Measurement not defined")
//    }
//
//    // Format the output
//    val output_pairs: RDD[((Int, String), (Int, String))] = verified_pairs
//      .map(x => (if (x._1 < x._2) x else (x._2, x._1)))
//      .sortByKey()
//      .map(x => ((x._1, tableMap(x._1)), (x._2, tableMap(x._2))))
//
//    output_pairs
//  }

}
