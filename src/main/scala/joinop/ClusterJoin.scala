package joinop

import java.util.Calendar
import org.apache.spark.rdd._
import org.apache.spark.HashPartitioner
import utils.Distance

/*
 * ClusterJoin is a clustering-based method for similarity join,
 * which guarantees load balancing with high probability.
 *
 * The implementation is scalable and extensible to any distance function
 * within the MapReduce framework.
 *
 * Reference: Das Sarma Akash, Clusterjoin: A similarity joins framework using map-reduce, VLDB 14'
 */

class ClusterJoin(measure: String, threshold: Double, anchorNum: Int) extends Serializable{
  var measureObj = new Distance()

  // Execute local join (verification) on each partition, performance depending on the parallelism
  def localJoin(cluster: Iterator[(Int, (String, (Int, String)))]): Iterator[((Int, String), (Int, String))] = {
    var result = List[((Int, String), (Int, String))]()
    var homePoints = List[(Int, String)]()

    while (cluster.hasNext) {
      val point = cluster.next
      for(prevPoint <- homePoints){
        // Apply length filtering
        if( (point._2._2._2.length - prevPoint._2.length) <= threshold)
          // Call the metric function to verify the pair
          if(measureObj.editDistance(prevPoint._2, point._2._2._2) <= threshold)
            result = result :+ (prevPoint, point._2._2)
      }
      if(point._2._1 == "home"){
        homePoints = homePoints :+ point._2._2
      }
    }

    result.iterator
  }

  def selfJoin(table: RDD[(Int, String)]): RDD[((Int, String), (Int, String))] = {
    // Randomly select the anchor points
    val anchors: Array[((Int, String), Int)] = table.takeSample(withReplacement = false, num = anchorNum).zipWithIndex

    // TODO: [Optimize Load Balancing] Estimate overloaded partition, and split it

    // anchorDist: Calculate the distance between all points and the anchors
    // RDD[(point, (minDist, minAnchorId))], for all the points
    val anchorDist: RDD[((Int, String), (Int, Int))] = table.map{
      case point@(pointId, pointVal) => (
        point,
        anchors.map{case anchor@(anchorPoint, anchorId) => measureObj.editDistance(pointVal, anchorPoint._2)}.zipWithIndex.min
      )
    }

    // partitionMapper: Map the points to their corresponding home partition and outer partitions
    // Note: AVOID unnecessary comparisons, see paper 7.2
    val partitionMapper = anchorDist.flatMap{
      case row@(point, (minDist, minAnchorId)) =>
        anchors.map{
          case anchor@(anchorPoint, anchorId) =>
            if (anchorId == minAnchorId)
              (anchorId, ("home", point))
            else if (((minAnchorId < anchorId) ^ (minAnchorId + anchorId) % 2 == 1) &&
                (measureObj.editDistance(point._2, anchorPoint._2) <= minDist + (2 * threshold).toInt))
              (anchorId, ("outer", point))
            else null
        }
    }
      .filter(x => x != null)
      .sortBy(x => (x._1, x._2._1), ascending = true)

    // Hash partitioning, by anchor index
    val partitions = partitionMapper.partitionBy(new HashPartitioner(anchorNum))

    // Execute local join (verification) on each partition, performance depending on the parallelism
    val verifiedPairs = partitions.mapPartitionsWithIndex(
      (index, partition) => {
        localJoin(partition)
      }
    ).repartition(1)

    verifiedPairs
  }
}
