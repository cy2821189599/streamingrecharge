package launch

import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD
import redis.clients.jedis.JedisCluster
import util.{JedisClusterUtil, PropertiesUtil}

object RechargeOrderProcess {


  def process(jsonRDD: RDD[JSONObject]) = {
    jsonRDD.foreachPartition(pt => {
      val cluster = JedisClusterUtil.getCluster()
      pt.foreach(
        json => {
          val serviceName = json.get("serviceName")
          serviceName match {
            case "reChargeNotifyReq" => totalRecharge(json, cluster)
            case _ =>
          }
        })
    }
    )
  }

  def orderCount(day: String, cluster: JedisCluster): Unit = {
    try {
      val key = PropertiesUtil.getProperty("rechargeCount", "jedisConfig.properties")
      cluster.hincrBy(day, key, 1)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  /**
   *
   * @param json
   */
  def totalRecharge(json: JSONObject, cluster: JedisCluster): Unit = {
    if (json.get("interFacRst").equals("0000")) {
      //充值日期
      val day = json.getString("requestId").substring(0, 8)
      //minute
      val minute = json.getString("requestId").substring(8, 12)
      minuteRecharge(day, minute, cluster)
      orderCount(day, cluster)
      // 获取充值时间
      val start = json.getString("requestId").substring(0, 17).toLong
      val end = json.getString("receiveNotifyTime").toLong
      val interval = end - start
      try {
        val key = PropertiesUtil.getProperty("rechargeMonney", "jedisConfig.properties")
        //充值成功
        if (json.get("bussinessRst").equals("0000")) {
          successRechargeCount(day, cluster)
          cluster.hincrByFloat(day, key, json.getDouble("chargefee"))
        }
        val timeKey = PropertiesUtil.getProperty("rechargeTime", "jedisConfig.properties")
        cluster.hincrBy(day, timeKey, interval)
      } catch {
        case exception: Exception => exception.printStackTrace()
      }
    }
  }

  def successRechargeCount(day: String, cluster: JedisCluster): Unit = {
    val key = PropertiesUtil.getProperty("successRechargeCount", "jedisConfig.properties")
    cluster.hincrBy(day, key, 1)
  }

  def minuteRecharge(day: String, minute: String, cluster: JedisCluster): Unit = {
    cluster.hincrBy(day, minute, 1)
  }

}
