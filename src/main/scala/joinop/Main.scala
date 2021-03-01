package joinop

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    // Join operation timer, usage: time{func}
    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block    // call-by-name
      val t1 = System.currentTimeMillis()
      println("Elapsed time: " + (t1 - t0) + "ms")
      result
    }

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
    val dataPath = "C:\\Users\\10750\\Desktop\\Dataset\\dblp_10K.csv"
    val threshold = 2
    val joinColumn = 0
    val metric = edit_distance(_, _)

    val spark = SparkSession
      .builder
      .appName("Join Test example")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(dataPath)
    val rdd = df.rdd

    // Full cartesian product similarity self-join (quadratic complexity)
    val t0 = System.nanoTime()
    val cart = rdd.map(x => (x(joinColumn), x))
        .cartesian(rdd.map(x => (x(joinColumn), x))) // [[key1, tuple1], [key2, tuple2]]
      .filter(x => (x._1._1.toString != x._2._1.toString // avoid duplication in self-join
        && metric(x._1._1.toString, x._2._1.toString) <= threshold))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/(Math.pow(10,9)) + "s")

    // Dump the result to text file
//    val result = cart.map(x => ((x._1._1.toString, x._2._1.toString))).coalesce(1).saveAsTextFile("result.txt")

    spark.stop()
  }
}
