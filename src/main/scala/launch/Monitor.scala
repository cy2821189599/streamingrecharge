package launch

import com.alibaba.fastjson.{JSON}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import util.{ContainKafkaOffset2Redis, PropertiesUtil}

/**
 *
 */
object Monitor {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("streaming").setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //开启后spark反压机制，自动根据系统负载选择最优消费速率
      .set("spark.streaming.backpressure.enabled", "true")
      // 当资源不足时或资源过剩时自动调节核心数
      .set("spark.dynamicAllocation.enabled", "true")
      //默认直接读取所有，在反压开启的情况下，限制第一次批处理应该消费的数据，因为程序冷启动，队列里面有大量积压，防止第一次全部读取，造成系统阻塞
      .set("spark.streaming.backpressure.initialRate", "1000")
      //默认直接读取所有,限制每秒每个消费线程读取每个kafka分区最大的数据量
      .set("spark.streaming.kafka.maxRatePerPartition", "120")
      //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      // 开启推测执行
      .set("spark.speculation", "true")
      .set("spark.speculation.interval", "60s")
      .set("spark.speculation.quantile", "0.9")

    val sc = new SparkContext(conf)

    val kafkaParams = Map(
      "bootstrap.servers" -> PropertiesUtil.getProperty("default.brokers", "kafkaConfig.properties"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> PropertiesUtil.getProperty("groupid", "kafkaConfig.properties"),
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false"
    )
    val topics = PropertiesUtil.getProperty("consumer.topic", "kafkaConfig.properties").split(",")
    val ssc = setupSsc(sc, topics, kafkaParams)

    ssc.start()
    ssc.awaitTermination()
  }

  def setupSsc(sparkContext: SparkContext, topics: Array[String], kafkaParam: Map[String, Object]) = {
    val ssc = new StreamingContext(sparkContext, Seconds(10))
    ssc.checkpoint("d:/temp/recharge")
    val group = PropertiesUtil.getProperty("groupid", "kafkaConfig.properties")
    val fromOffsets = ContainKafkaOffset2Redis.getOffSets(group)
    var stream: InputDStream[ConsumerRecord[String, String]] = null
    if (fromOffsets.size > 0) {
      stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,
        String](topics, kafkaParam, fromOffsets))
    } else {
      stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies
        .Subscribe[String,
          String](topics, kafkaParam))
    }
    //广播变量
    var broadcastMap = Map[String, Map[String, String]]()
    val province = broadCastProcess.getProvinceData("D:\\ReciveFile\\province.txt")
    broadcastMap += ("province" -> province)
    val broadcast = sparkContext.broadcast(broadcastMap)

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      ContainKafkaOffset2Redis.updateOffset(offsetRanges, group)
      val jsonRDD = rdd.map(_.value()).map(jsonStr => JSON.parseObject(jsonStr)).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //业务概况
      RechargeOrderProcess.process(jsonRDD)
    })

    val jsonDStream = stream.mapPartitions(partition => {
      partition.map(_.value()).map(JSON.parseObject(_))
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //业务质量-失败
    RechargeQualify.summerize(jsonDStream, broadcast)
    //top10
    RechargeQualify.top10Province(jsonDStream, broadcast)
    // 实时统计每小时的充值笔数和金额
    RechargeQualify.perHourRecharge(jsonDStream)

    /*
    val lines = sparkContext.textFile("D:\\ReciveFile\\项目3\\充值平台实时统计分析\\cmcc.json")
    val jsonRDD = lines.map(line => JSON.parseObject(line)).persist
    (StorageLevel.MEMORY_AND_DISK_SER)
    //充值请求-省份维度
    RechargeRequest.computeProvinceDimension(jsonRDD, broadcast)
    //充值请求-省份-每分钟
    RechargeRequest.fromProvinceAndMinuteDimension(jsonRDD, broadcast)
    // 充值请求-省份-每小时
    RechargeRequest.fromProvinceAndHourDimension(jsonRDD, broadcast)

     */

    ssc
  }
}
