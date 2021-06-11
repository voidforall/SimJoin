package joinop

import java.util.Calendar
import java.io._
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

class ClusterJoin(measure: String, threshold: Double, anchorNum: Int, stepReport: Boolean, detailReport: Boolean) extends Serializable{
  var measureObj = new Distance()

  // Execute local join (verification) on each partition, performance depending on the parallelism
  def localJoin(cluster: Iterator[(Int, (String, (Int, String)))]): Iterator[((Int, String), (Int, String))] = {
    var result = List[((Int, String), (Int, String))]()
    var homePoints = List[(Int, String)]()

    while (cluster.hasNext) {
      val point = cluster.next
      for(prevPoint <- homePoints){
        // Apply length filtering
        if( (point._2._2._2.length - prevPoint._2.length) <= threshold) {
          // Call the metric function to verify the pair
          measure match{
            case "ED" => {
              if(measureObj.editDistance(prevPoint._2, point._2._2._2) <= threshold)
                result = result :+ (prevPoint, point._2._2)
            }
            case "Jaccard" => {
              if(measureObj.jaccard(prevPoint._2.split(" ").toSet, point._2._2._2.split(" ").toSet) <= threshold)
                result = result :+ (prevPoint, point._2._2)
            }
          }
        }
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
    val anchorDist: RDD[((Int, String), (Double, Int))] = table.map{
      case point@(pointId, pointVal) => (
        point,
        measure match{
          case "ED" => anchors.map{case anchor@(anchorPoint, anchorId) => measureObj.editDistance(pointVal, anchorPoint._2)}.map(_.toDouble).zipWithIndex.min
          case "Jaccard" => anchors.map{case anchor@(anchorPoint, anchorId) => measureObj.jaccard(pointVal.split(" ").toSet, anchorPoint._2.split(" ").toSet)}.zipWithIndex.min
        }
      )
    }

    // partitionMapper: Map the points to their corresponding home partition and outer partitions
    // Note: AVOID unnecessary comparisons, see paper 7.2
    val partitionMapper = {
    measure match {
      case "ED" =>
        anchorDist.flatMap{
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
      case "Jaccard" =>
        anchorDist.flatMap{
          case row@(point, (minDist, minAnchorId)) =>
            anchors.map{
              case anchor@(anchorPoint, anchorId) =>
                if (anchorId == minAnchorId)
                  (anchorId, ("home", point))
                else if (((minAnchorId < anchorId) ^ (minAnchorId + anchorId) % 2 == 1) &&
                  (measureObj.jaccard(point._2.split(" ").toSet, anchorPoint._2.split(" ").toSet) <= minDist + 2 * threshold))
                  (anchorId, ("outer", point))
                else null
            }
        }
          .filter(x => x != null)
          .sortBy(x => (x._1, x._2._1), ascending = true)
      }
    }

    if (detailReport) {
      val home_partition_list = partitionMapper.filter(x => x._2._1 == "home").countByKey.values.toList
      val home_file = "/scratch/yuan/data/log_data/home.txt"
      val home_writer = new BufferedWriter(new FileWriter(home_file))
      for (i <- home_partition_list)
        home_writer.write(i.toString + "\n")
      home_writer.close()

      val outer_partition_list = partitionMapper.filter(x => x._2._1 == "outer").countByKey.values.toList
      val outer_file = "/scratch/yuan/data/log_data/outer.txt"
      val outer_writer = new BufferedWriter(new FileWriter(outer_file))
      for (i <- outer_partition_list)
        outer_writer.write(i.toString + "\n")
      outer_writer.close()

//      println("[ClusterJoin] Partition pairs stats: " + )
//      println("[ClusterJoin] Total candidate pairs: " + )
    }

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
