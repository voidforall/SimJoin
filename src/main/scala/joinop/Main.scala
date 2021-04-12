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

    // Parameter Setting
    val dataPath = "/scratch/yuan/data/words_2K.csv"
//    val dataPath = "C:\\Users\\10750\\Desktop\\Dataset\\words_1K.csv"
    val threshold = 1
    val metric = edit_distance _

    val spark = SparkSession
      .builder
      .appName("Join Test example")
//      .config("spark.master", "local")
      .getOrCreate()

    // Prepare data
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
      .map{case(a, b) => (b.toInt, a.toString)}

    val ts1 = Calendar.getInstance().getTimeInMillis
    // Cartesian product based similarity self-join (exactly quadratic complexity)
    // Cartesian-product result can be used to verify other methods' correctness
    val cart_result: RDD[((Int, String), (Int, String))] =
      table.cartesian(table)
        .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
        .distinct
        .filter(x => (x._1._1 != x._2._1 // avoid duplication in self-join
          && metric(x._1._2, x._2._2) <= threshold)) // threshold verification
    val cart_count = cart_result.count
    val te1 = Calendar.getInstance().getTimeInMillis
    println("[Cartesian] Elapsed time: " + (te1 - ts1) / 1000.0 + "s")
    println("[Cartesian] Matched pairs: " + cart_count)

//    val ts2 = Calendar.getInstance().getTimeInMillis
//    val prefixJoin = new PrefixJoin(measure="ED", threshold=1, ordering="idf", tokenize="qgram", q=4)
//    val prefix_result: RDD[((Int, String), (Int, String))] = prefixJoin.selfJoin(table)
//    val prefix_count = prefix_result.count
//    val te2 = Calendar.getInstance().getTimeInMillis
//    println("[PrefixJoin] Elapsed time: " + (te2 - ts2) / 1000.0 + "s")
//    println("[Prefix] Matched pairs: " + prefix_count)
//
//    val ts3 = Calendar.getInstance().getTimeInMillis
//    val edJoin = new EDJoin(measure="ED", threshold=1, ordering="idf", tokenize="qgram", q=4)
//    val ed_result: RDD[((Int, String), (Int, String))] = edJoin.selfJoin(table)
//    val ed_count = ed_result.count
//    val te3 = Calendar.getInstance().getTimeInMillis
//    println("[EDJoin] Elapsed time: " + (te3 - ts3) / 1000.0 + "s")
//    println("[EDJoin] Matched pairs: " + ed_count)

//    val ts4 = Calendar.getInstance().getTimeInMillis
//    val countJoin = new GramCountJoin(threshold=1, q=5)
//    val count_result: RDD[((Int, String), (Int, String))] = countJoin.selfJoin(table)
//    val count_count = count_result.count
//    val te4 = Calendar.getInstance().getTimeInMillis
//    println("[CountJoin] Elapsed time: " + (te4 - ts4) / 1000.0 + "s")
//    println("[CountJoin] Matched pairs: " + count_count)
    
    val ts5 = Calendar.getInstance().getTimeInMillis
    // Cartesian product based similarity self-join (exactly quadratic complexity)
    // Cartesian-product result can be used to verify other methods' correctness
    val clusterJoin = new ClusterJoin(measure="ED", threshold=1, anchorNum=4)
    val cluster_result: RDD[((Int, String), (Int, String))] = clusterJoin.selfJoin(table)
    val cluster_count = cluster_result.count
    val te5 = Calendar.getInstance().getTimeInMillis
    println("[ClusterJoin] Elapsed time: " + (te5 - ts5) / 1000.0 + "s")
    println("[ClusterJoin] Matched pairs: " + cluster_count)

    // strict verification, however too long time for large-scale dataset
    // we will only verify the correctness on small-scale dataset, and test performance on large-scale ones
//    println(prefix_result.subtract(cart_result).count)

    // Dump the result to text file
//    cart_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("cart.txt")
//    prefix_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("prefix.txt")
//    ed_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("edjoin.txt")
//    count_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("count.txt")
//    cluster_result.map(x => (x._1._2, x._2._2)).coalesce(1).saveAsTextFile("cluster.txt")

//    System.in.read() // wait to investigate Spark UI
    spark.stop()
  }
}
