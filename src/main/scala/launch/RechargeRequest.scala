package launch

import java.text.DecimalFormat

import com.alibaba.fastjson.JSONObject
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import util.C3p0Util

object RechargeRequest {

  def computeProvinceDimension(jsonRDD: RDD[JSONObject], broadcast: Broadcast[Map[String, Map[String, String]]]): Unit = {
    val result = jsonRDD.filter(x => x.getString("serviceName").equalsIgnoreCase("sendRechargeReq")
      && x.getString("interFacRst").equals("0000"))
      .mapPartitions(partition => {
        val provinceMap = broadcast.value.get("province").get
        partition.map(json => {
          val provinceCode = json.getString("provinceCode")
          val provinceName = provinceMap.get(provinceCode).get
          val fail = if (!json.getString("bussinessRst").equals("0000")) 1 else 0
          (provinceName, fail)
        })
      }).groupByKey(34).map(pro => {
      val itr = pro._2
      val size = itr.size
      val fail_cnt = itr.reduce(_ + _)
      val rate: Float = fail_cnt.toFloat / size
      val decimalFormat = new DecimalFormat("#0.0")
      val formatted = decimalFormat.format(rate)
      (pro._1, fail_cnt, formatted)
    }).collect()
    val connection = C3p0Util.getConnection
    for (elem <- result) {
      val sql = "insert into fail_RechargeReq(province,fail_cnt,fail_rate) values(?,?,?)"
      val statement = connection.prepareStatement(sql)
      statement.setString(1, elem._1)
      statement.setInt(2, elem._2)
      statement.setString(3, elem._3)
      statement.executeUpdate()
    }
  }

  def fromProvinceAndMinuteDimension(jsonRDD: RDD[JSONObject],
                                     broadcast: Broadcast[Map[String, Map[String, String]]]): Unit = {
    val result = jsonRDD.filter(x => x.getString("serviceName").equalsIgnoreCase("sendRechargeReq")
      && x.getString("interFacRst").equals("0000") && x.getString("bussinessRst").equals("0000"))
      .mapPartitions(partition => {
        val provinceMap = broadcast.value.get("province").get
        partition.map(json => {
          val provinceCode = json.getString("provinceCode")
          val provinceName = provinceMap.get(provinceCode).get
          val minute = json.getString("requestId").substring(0, 12)
          val money = json.getDouble("chargefee")
          ((provinceName, minute), money)
        })
      }).groupByKey(34).map(pro => {
      val size = pro._2.size
      val money = pro._2.reduce(_ + _)
      (pro._1._1, pro._1._2, size, money)
    }).collect()
    val connection = C3p0Util.getConnection
    val sql = "insert into provinceReq_minute(province,minute,recharge_cnt,money) values(?,?,?,?)"
    var statement = connection.prepareStatement(sql)
    for (elem <- result) {
      statement.setString(1, elem._1)
      statement.setString(2, elem._2)
      statement.setInt(3, elem._3)
      statement.setDouble(4, elem._4)
      statement.executeUpdate()
    }
  }

  def fromProvinceAndHourDimension(jsonRDD: RDD[JSONObject],
                                   broadcast: Broadcast[Map[String, Map[String, String]]]): Unit = {
    val result = jsonRDD.filter(x => x.getString("serviceName").equalsIgnoreCase("sendRechargeReq")
      && x.getString("interFacRst").equals("0000") && x.getString("bussinessRst").equals("0000"))
      .mapPartitions(partition => {

        val provinceMap = broadcast.value.get("province").get
        partition.map(json => {
          val provinceCode = json.getString("provinceCode")
          val provinceName = provinceMap.get(provinceCode).get
          val hour = json.getString("requestId").substring(0, 10)
          val money = json.getDouble("chargefee")
          ((provinceName, hour), money)
        })
      }).groupByKey(34).map(pro => {
      val size = pro._2.size
      val money = pro._2.reduce(_ + _)
      (pro._1._1, pro._1._2, size, money)
    }).collect()
    val connection = C3p0Util.getConnection
    val sql = "insert into provinceReq_hour(province,hour,recharge_cnt,money) values(?,?,?,?)"
    var statement = connection.prepareStatement(sql)
    for (elem <- result) {
      statement.setString(1, elem._1)
      statement.setString(2, elem._2)
      statement.setInt(3, elem._3)
      statement.setDouble(4, elem._4)
      statement.executeUpdate()
    }
  }

}
