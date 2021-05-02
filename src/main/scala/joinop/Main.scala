package joinop

import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql._

object Main {
  def main(args: Array[String]) {

    // Edit distance metric given two strings
    def edit_distance(s1: String, s2: String): Int = {
      val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

      @inline
      def minimum(i: Int*): Int = i.min

      for {j <- dist.indices.tail
           i <- dist(0).indices.tail} dist(j)(i) =
        if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
        else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

      dist(s2.length)(s1.length)
    }

    // Experiment Setting
    val experiments = collection.immutable.Map(
      "Cartesian" -> false,
      "PrefixJoin" -> true,
      "EDJoin" -> true,
      "GramCount" -> false,
      "ClusterJoin" -> true
    )

    // Parameter Setting
//    val dataPath = "/scratch/yuan/data/words_q4_117.3.csv"
    val dataPath = "C:\\Users\\10750\\Desktop\\Dataset\\words_1K.csv"
    val threshold = 1
    val metric = edit_distance _

    val spark = SparkSession
      .builder
      .appName("Similarity Join Algorithms Evaluation")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // Prepare data as cached RDD
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
      .map{case(a, b) => (b.toInt, a.toString)}.cache()

    // 1. Cartesian product based similarity self-join (exactly quadratic complexity)
    //    p.s. Cartesian-product result can be used to verify other methods' correctness
    val cart_ts = Calendar.getInstance().getTimeInMillis
    var cart_count = 0L
    var cart_te = 0L
    if (experiments.getOrElse("Cartesian", false)) {
      val cart_result: RDD[((Int, String), (Int, String))] =
        table.cartesian(table)
          .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
          .distinct
          .filter(x => (x._1._1 != x._2._1 // avoid duplication in self-join
            && metric(x._1._2, x._2._2) <= threshold)) // threshold verification
      cart_count = cart_result.count
      cart_te = Calendar.getInstance().getTimeInMillis
    }

    // 2. Prefix Join similarity join, general framework with no optimization
    val prefix_ts = Calendar.getInstance().getTimeInMillis
    var prefix_count = 0L
    var prefix_te = 0L
    if (experiments.getOrElse("PrefixJoin", false)) {
      val prefixJoin = new PrefixJoin(measure = "ED", threshold = 1, ordering = "idf", tokenize = "qgram", q = 4, stepReport = false)
      val prefix_result: RDD[((Int, String), (Int, String))] = prefixJoin.selfJoin(table)
      prefix_count = prefix_result.count
      prefix_te = Calendar.getInstance().getTimeInMillis
    }

    // 3. EDJoin similarity join, optimized version of prefix join
    val ed_ts = Calendar.getInstance().getTimeInMillis
    var ed_count = 0L
    var ed_te = 0L
    if (experiments.getOrElse("EDJoin", false)) {
      val edJoin = new EDJoin(measure = "ED", threshold = 1, ordering = "idf", tokenize = "qgram", q = 4, stepReport = false)
      val ed_result: RDD[((Int, String), (Int, String))] = edJoin.selfJoin(table)
      ed_count = ed_result.count
      ed_te = Calendar.getInstance().getTimeInMillis
    }

    // 4. GramCount similarity join, hard to terminate in expected timeout
    val gram_ts = Calendar.getInstance().getTimeInMillis
    var gram_count = 0L
    var gram_te = 0L
    if (experiments.getOrElse("GramCount", false)) {
      val countJoin = new GramCountJoin(threshold = 1, q = 5)
      val count_result: RDD[((Int, String), (Int, String))] = countJoin.selfJoin(table)
      gram_count = count_result.count
      gram_te = Calendar.getInstance().getTimeInMillis
    }

    // 5. ClusterJoin similarity join, with good capacity in parallelization
    val cluster_ts = Calendar.getInstance().getTimeInMillis
    var cluster_count = 0L
    var cluster_te = 0L
    if (experiments.getOrElse("ClusterJoin", false)) {
      val clusterJoin = new ClusterJoin(measure = "ED", threshold = 1, anchorNum = 50)
      val cluster_result = clusterJoin.selfJoin(table)
      cluster_count = cluster_result.count
      cluster_te = Calendar.getInstance().getTimeInMillis
    }

    if (experiments.getOrElse("Cartesian", false)) {
      println("[Cartesian] Elapsed time: " + (cart_te - cart_ts) / 1000.0 + "s")
      println("[Cartesian] Matched pairs: " + cart_count)
    }

    if (experiments.getOrElse("PrefixJoin", false)) {
      println("[PrefixJoin] Elapsed time: " + (prefix_te - prefix_ts) / 1000.0 + "s")
      // TODO: report the verification times
      //    println("[PrefixJoin] The string similarity measure called times: " + prefixJoin.measureObj.callerTimes)
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

    // strict verification, however too long time for large-scale dataset
    // we will only verify the correctness on small-scale dataset, and test performance on large-scale ones
//    println(prefix_result.subtract(cart_result).count)

    // Dump the result to text file
//    cart_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("cart.txt")
//    prefix_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("prefix.txt")
//    ed_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("edjoin.txt")
//    gram_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("count.txt")
//    cluster_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("cluster.txt")

//    System.in.read() // wait to investigate Spark UI
    spark.stop()
  }
}
