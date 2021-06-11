package joinop

import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.col

import utils.Distance

object Main {
  def main(args: Array[String]) {
    // Experiment Setting
    val experiments = collection.immutable.Map(
      "Cartesian" -> false,
      "PrefixJoin" -> true,
      "EDJoin" -> true,
      "GramCount" -> false,
      "ClusterJoin" -> true,
      "LSH" -> false
    )

    // Parameter Setting
    val dataPath = "/scratch/yuan/data/skew91_10K.csv"

    val threshold = 1
    val qgram_size = 4
    val cluster_num = 50
    val metric_type = "ED" // Options: ["ED", "Jaccard", "Cosine", "Dice"]
    var measureObj = new Distance()

    val spark = SparkSession
      .builder
      .appName("Similarity Join Algorithms Evaluation")
//      .config("spark.master", "local[*]")
      .getOrCreate()

    // Prepare data as cached RDD, make sure that the rdd has default partition number
    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(dataPath)
    val rdd: RDD[Row] = df.rdd
    val table: RDD[(Int, String)] =
      rdd
      .zipWithIndex()
      .map{case(a, b) => (b.toInt, a.toString)}.repartition(spark.sparkContext.defaultParallelism).cache()

    // 1. Cartesian product based similarity self-join (exactly quadratic complexity)
    //    p.s. Cartesian-product result can be used to verify other methods' correctness
    val cart_ts = Calendar.getInstance().getTimeInMillis
    var cart_count = 0L
    var cart_te = 0L
    if (experiments.getOrElse("Cartesian", false)) {
      if (metric_type == "ED"){
        val cart_result: RDD[((Int, String), (Int, String))] =
          table.cartesian(table)
            .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
            .distinct
            .filter(x => (x._1._1 != x._2._1 // avoid duplication in self-join
//              && measureObj.editDistance(x._1._2, x._2._2) <= threshold
              )) // threshold verification
        cart_count = cart_result.count
        cart_te = Calendar.getInstance().getTimeInMillis
      }
      else if (metric_type == "Jaccard"){
        val cart_result: RDD[((Int, String), (Int, String))] =
          table.cartesian(table)
            .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
            .distinct
            .filter(x => (x._1._1 != x._2._1 // avoid duplication in self-join
              && measureObj.jaccard(x._1._2.split(" ").toSet, x._2._2.split(" ").toSet) <= threshold)) // threshold verification
        cart_count = cart_result.count
        cart_te = Calendar.getInstance().getTimeInMillis
//        cart_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("cart.txt")
      }
    }

    var tokenize = ""
    if (metric_type == "ED")
      tokenize = "qgram"
    else
      tokenize = "space"

    // 2. Prefix Join similarity join, general framework with no optimization
    val prefix_ts = Calendar.getInstance().getTimeInMillis
    var prefix_count = 0L
    var prefix_te = 0L
    if (experiments.getOrElse("PrefixJoin", false)) {
      val prefixJoin = new PrefixJoin(measure = metric_type, threshold = threshold, ordering = "idf", tokenize = tokenize, q = qgram_size, stepReport = true, detailReport = true)
      val prefix_result: RDD[((Int, String), (Int, String))] = prefixJoin.selfJoin(table)
      prefix_count = prefix_result.count
      prefix_te = Calendar.getInstance().getTimeInMillis
      //      prefix_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("prefix.txt")
    }

    // 3. EDJoin similarity join, optimized version of prefix join
    val ed_ts = Calendar.getInstance().getTimeInMillis
    var ed_count = 0L
    var ed_te = 0L
    if (experiments.getOrElse("EDJoin", false)) {
      val edJoin = new EDJoin(measure = "ED", threshold = threshold, ordering = "idf", tokenize = tokenize, q = qgram_size, stepReport = true, detailReport = true)
      val ed_result: RDD[((Int, String), (Int, String))] = edJoin.selfJoin(table)
      ed_count = ed_result.count
      ed_te = Calendar.getInstance().getTimeInMillis
    }

    // 4. GramCount similarity join, hard to terminate in expected timeout
    val gram_ts = Calendar.getInstance().getTimeInMillis
    var gram_count = 0L
    var gram_te = 0L
    if (experiments.getOrElse("GramCount", false)) {
      val countJoin = new GramCountJoin(threshold = 1, q = qgram_size)
      val count_result: RDD[((Int, String), (Int, String))] = countJoin.selfJoin(table)
      gram_count = count_result.count
      gram_te = Calendar.getInstance().getTimeInMillis
    }

    // 5. ClusterJoin similarity join, with good capacity in parallelization
    val cluster_ts = Calendar.getInstance().getTimeInMillis
    var cluster_count = 0L
    var cluster_te = 0L
    if (experiments.getOrElse("ClusterJoin", false)) {
      val clusterJoin = new ClusterJoin(measure = metric_type, threshold = threshold, anchorNum = cluster_num, stepReport = true, detailReport = true)
      val cluster_result = clusterJoin.selfJoin(table)
      cluster_count = cluster_result.count
      cluster_te = Calendar.getInstance().getTimeInMillis
    }

    // 6. LSH for approximate sim join
    var lsh_ts = Calendar.getInstance().getTimeInMillis
    var lsh_count = 0L
    var lsh_te = 0L
    var precision = 0.0
    var recall = 0.0
    if (experiments.getOrElse("LSH", false)) {
      val table_tokenized = table.map(x => (x._1, x._2.split(" ").toArray))
      val vocab: Map[String, Int] = table_tokenized.flatMap {
        case (id, qgrams) => qgrams
      }
        .groupBy(x => x)
        .map(x => (x._1, x._2.size))
        .collectAsMap().toMap
      val vocab_id = table_tokenized.context.broadcast(vocab.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)

      val table_ids = (table_tokenized.map { case (id, qgrams) => (id, qgrams.map(x => vocab_id.value(x)).sortBy(x => x)) })
        .map(x => (x._1, Vectors.sparse(vocab.size, x._2, x._2.map(x => 1.0))))
      val df_tokenized = spark.createDataFrame(table_ids).toDF("id", "features")

      val mh = new MinHashLSH()
        .setNumHashTables(10)
        .setInputCol("features")
        .setOutputCol("hashes")
      val model = mh.fit(df_tokenized)
      lsh_ts = Calendar.getInstance().getTimeInMillis
      val lsh_result = model.approxSimilarityJoin(model.transform(df_tokenized), model.transform(df_tokenized), threshold, "JaccardDistance")
        .select(col("datasetA.id").alias("idA"),
          col("datasetB.id").alias("idB"))
        .asInstanceOf[DataFrame]
        .filter("idA < idB and idA != idB").rdd
        .map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))

      lsh_count = lsh_result.count
      lsh_te = Calendar.getInstance().getTimeInMillis

      // Evaluate precision/recall of LSH results
      val prefixJoin = new PrefixJoin(measure = metric_type, threshold = threshold, ordering = "idf", tokenize = tokenize, q = qgram_size, stepReport = false, detailReport = false)
      val ground_truth: RDD[(Int, Int)] = prefixJoin.selfJoin(table).map(x => (x._1._1, x._2._1))
      precision = lsh_result.intersection(ground_truth).count.toDouble / lsh_count.toDouble
      recall = lsh_result.intersection(ground_truth).count.toDouble / ground_truth.count.toDouble
    }

    // Report execution time and number of pairs
    if (experiments.getOrElse("Cartesian", false)) {
      println("[Cartesian] Elapsed time: " + (cart_te - cart_ts) / 1000.0 + "s")
      println("[Cartesian] Matched pairs: " + cart_count)
    }

    if (experiments.getOrElse("PrefixJoin", false)) {
      println("[PrefixJoin] Elapsed time: " + (prefix_te - prefix_ts) / 1000.0 + "s")
      println("[Prefix] Matched pairs: " + prefix_count)
    }

    if (experiments.getOrElse("EDJoin", false)) {
      println("[EDJoin] Elapsed time: " + (ed_te - ed_ts) / 1000.0 + "s")
      println("[EDJoin] Matched pairs: " + ed_count)
    }

    if (experiments.getOrElse("GramCount", false)) {
      println("[GramCount] Elapsed time: " + (gram_te - gram_ts) / 1000.0 + "s")
      println("[GramCount] Matched pairs: " + gram_count)
    }

    if (experiments.getOrElse("ClusterJoin", false)) {
      println("[ClusterJoin] Elapsed time: " + (cluster_te - cluster_ts) / 1000.0 + "s")
      println("[ClusterJoin] Matched pairs: " + cluster_count)
    }

    if (experiments.getOrElse("LSH", false)) {
      println("[LSH] Elapsed time: " + (lsh_te - lsh_ts) / 1000.0 + "s")
      println("[LSH] Matched pairs: " + lsh_count)
      println("[LSH] Precision: " + precision)
      println("[LSH] Recall: " + recall)
    }

    // strict verification, however too long time for large-scale dataset
    // we will only verify the correctness on small-scale dataset, and test performance on large-scale ones
//    println(prefix_result.subtract(cart_result).count)

    // Dump the result to text file
//    cart_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("cart.txt")
//    prefix_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("prefix.txt")
//    ed_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("edjoin.txt")
//    gram_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("count.txt")
//    cluster_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("cluster.txt")

    // wait to investigate Spark UI
//    System.in.read()

    spark.stop()
  }
}
