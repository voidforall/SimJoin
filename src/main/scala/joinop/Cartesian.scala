package joinop

import org.apache.spark.rdd._

class Cartesian(measure: String, threshold: Double) extends Serializable{
//  def edSelfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] ={
//    table.cartesian(table)
//      .filter(x => (x._1._1.toString != x._2._1.toString
//        && measure(x._1._1.toString, x._2._1.toString) <= threshold))
//      .map(x => (x._1._1, (x._1._2, x._2._2)))
//  }
//
//  def edRSJoin(other: RDD[(K, W)]):  ={
//    rdd.cartesian(other)
//      .filter(x => measure(x._1._1.toString, x._2._1.toString) <= threshold)
//      .map(x => (x._1._1, (x._1._2, x._2._2)))
//  }
}
