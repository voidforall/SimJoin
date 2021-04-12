package joinop

import java.util.Calendar
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd._
import org.apache.spark.HashPartitioner
import utils.Distance


class ClusterJoin(measure: String, threshold: Double, anchorNum: Int) extends Serializable{
  protected val measureObj = new Distance()

  // Approximate outer partition membership
  // Ref: paper 6.2 General filter for any metric distance
  def generalFilter(query: String, center: String, test: String): Boolean = {
    if(measureObj.editDistance(query, test) > measureObj.editDistance(query, center) + 2*threshold)
      false
    else
      true
  }

  def selfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
    // Randomly select the anchor points, given the sampling fraction
    val anchorProb = anchorNum / table.count().toDouble
    val anchors: RDD[(Int, String)] = table.sample(withReplacement=false, fraction=anchorProb, seed=42)

    // Calculate the distance between all points and the anchors
    val anchorDist: RDD[(Int, ((Int, String), (Int, String), Int))] = table.cartesian(anchors).map{
      case (point, anchor) =>
        (point._1, (point, anchor, measureObj.editDistance(point._2, anchor._2)))
    }

    // Assign the points to one cluster center, i.e., home partition
    val homePartition: RDD[((Int, String), (Int, String), Int)] = anchorDist.reduceByKey{
      case (point1, point2) =>
        if(point1._3 < point2._3) point1 else point2
    }.map{
      x => x._2
    }

    // Approximate outer partition membership
    val outerPartition: RDD[((Int, String), (Int, String))] = homePartition.cartesian(anchors).filter {
      case (assigned, anchor) => generalFilter(assigned._1._2, assigned._2._2, anchor._2)
    }.map{
      case (assigned, anchor) => (anchor, assigned._1)
    }

    // RDD partitioning
    val partitioning = outerPartition.partitionBy(new HashPartitioner(anchorNum))

    // In-partition (outer partition) verification by anchor points
    val verified_pairs: RDD[((Int, String), (Int, String))] = partitioning.mapPartitions(x =>
    {
      // find the similar pairs in each cluster
      var result: ListBuffer[((Int, String), (Int, String))] = ListBuffer.empty
      var inner: ListBuffer[(Int, String)] = ListBuffer.empty

      while(x.hasNext)
      {
        val pair = x.next()
        val currentPoint = pair._2._2
        for(joinPoint <- inner)
        {
          if(measureObj.editDistance(joinPoint._2, currentPoint) <= threshold)
            result.append((joinPoint, pair._2))
        }
        inner.append(pair._2)
      }
      result.iterator
    }).repartition(1)

    // Format the output
    val output_pairs: RDD[((Int, String), (Int, String))] = verified_pairs
      .filter(x => x._1._1 != x._2._1)
      .map(x => if (x._1._1 < x._2._1) x else (x._2, x._1))
      .sortByKey()
      .distinct()

    output_pairs
  }

}
