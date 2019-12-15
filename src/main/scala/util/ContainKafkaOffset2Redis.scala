package util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{OffsetRange}

object ContainKafkaOffset2Redis {


  def getOffSets(consumerGroup: String) = {
    var offsetMap = Map[TopicPartition, Long]()
    try {
      val cluster = JedisClusterUtil.getCluster()
      val topicPartitionOffsetsMap = cluster.hgetAll(consumerGroup)

      //group:topic-partition:offset
      val partitions = topicPartitionOffsetsMap.keySet()
      import scala.collection.JavaConversions._
      for (tp <- partitions) {
        val topicAndPartition = tp.split("-")
        offsetMap += (new TopicPartition(topicAndPartition(0), topicAndPartition(1).toInt) -> topicPartitionOffsetsMap.get
        (tp).toLong)
      }
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
    offsetMap
  }

  def updateOffset(offsetRanges: Array[OffsetRange], GROUP_ID: String): Unit = {
    try {
      val cluster = JedisClusterUtil.getCluster()
      for (o <- offsetRanges) {
        cluster.hset(GROUP_ID, o.topic + "-" + o.partition.toString, o.untilOffset.toString)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
