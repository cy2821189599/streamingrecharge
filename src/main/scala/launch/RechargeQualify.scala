package launch

import java.text.DecimalFormat

import com.alibaba.fastjson.JSONObject
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import util.C3p0Util

object RechargeQualify {


  def mappingFunction(key: (String, String), value: Option[Int], state: State[Int]) = {
    val count = value.getOrElse(0) + state.getOption().getOrElse(0)
    state.update(count)
    (key, count)
  }

  def summerize(jsonDStream: DStream[JSONObject], broadcast: Broadcast[Map[String, Map[String, String]]]) = {
    jsonDStream.filter(x => x.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq") &&
      x.getString("interFacRst").equals("0000") && !x.getString("bussinessRst").equals("0000"))
      .map(json => {
        val codeMap = broadcast.value.get("province").get
        val hour = json.getString("requestId").substring(0, 10)
        val provinceCode = json.getString("provinceCode")
        val provinceName = codeMap.get(provinceCode).get
        ((hour, provinceName), 1)
      }).reduceByKey((x: Int, y: Int) => x + y, 34).mapWithState(StateSpec.function(mappingFunction _))
      .foreachRDD(rdd => {
        val connection = C3p0Util.getConnection
        val result = rdd.collect()
        result.foreach(res => {
          @transient var sql: String = null
          //先做查询判断
          sql = "select count(*) cnt from failprovince where hour = ? and province = ?"
          var statement = connection.prepareStatement(sql)
          statement.setString(1, res._1._1)
          statement.setString(2, res._1._2)
          val resultSet = statement.executeQuery()
          var cnt = -1
          if (resultSet.next()) {
            cnt = resultSet.getInt("cnt")
          }
          val hourAndProvince = res._1
          //在更新插入
          if (cnt > 0) {
            sql = "update failprovince set cnt =? where hour=? and province =?"
            statement = connection.prepareStatement(sql)
            statement.setInt(1, res._2)
            statement.setString(2, hourAndProvince._1)
            statement.setString(3, hourAndProvince._2)
            val i = statement.executeUpdate()
          } else {
            sql = "insert into failprovince(hour,province,cnt) values(?,?,?)"
            statement = connection.prepareStatement(sql)
            statement.setString(1, hourAndProvince._1)
            statement.setString(2, hourAndProvince._2)
            statement.setInt(3, res._2)
            val x = statement.executeUpdate()
          }

        })
      })
  }

  /**
   * 记住要更新状态
   *
   * @param key
   * @param value
   * @param state
   * @return
   */
  def top10MappingFunction(key: String, value: Option[(Int, Int)], state: State[(Int, Int)]): (String, (Int, Int)) = {
    val size = value.getOrElse((0, 0))._1 + state.getOption().getOrElse((0, 0))._1
    val success = value.getOrElse((0, 0))._2 + state.getOption().getOrElse((0, 0))._2
    state.update((size, success))
    (key, (size, success))
  }

  /**
   * 这里卡了很久，是因为rdd数据被remove了导致执行不下去，卡在那了，然后通过缓存中间结果解决
   * 感觉还是因为数据库连接池初始化时间太久了的原因，通过查看日志发现很晚才打印连接池对象
   *
   * @param jsonDStream
   * @param broadcast
   */
  def top10Province(jsonDStream: DStream[JSONObject], broadcast: Broadcast[Map[String, Map[String, String]]]) = {
    jsonDStream.filter(x => x.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq") &&
      x.getString("interFacRst").equals("0000"))
      .mapPartitions(partition => {
        val provinceMap = broadcast.value.get("province").get
        partition.map(json => {
          val provinceCode = json.getString("provinceCode")
          val provinceName = provinceMap.get(provinceCode).get
          val success = if (json.getString("bussinessRst").equals("0000")) 1 else 0
          (provinceName, success)
        })
      }).groupByKey(34).map(pro => {
      // 订单总量
      val size = pro._2.size
      // 成功订单数量
      val successed = pro._2.reduce(_ + _)
      (pro._1, (size, successed))
    }).mapWithState(StateSpec.function(top10MappingFunction _))
      .map(pro => {
        val rate: Float = pro._2._2.toFloat / pro._2._1
        val decimalFormat = new DecimalFormat("#0.0")
        val rateFormatted = decimalFormat.format(rate)
        (pro._1, pro._2._1, rateFormatted)
      }).foreachRDD(rdd => {
      val top10 = rdd.sortBy(_._2, false).take(10)
      try {
        val connection = C3p0Util.getConnection
        for (elem <- top10) {
          var sql = "select count(*) cnt from provinceTop10 where province =?"
          var statement = connection.prepareStatement(sql)
          statement.setString(1, elem._1)
          val resultSet = statement.executeQuery()
          var cnt = -1
          if (resultSet.next()) {
            cnt = resultSet.getInt("cnt")
          }
          if (cnt > 0) {
            sql = "update provinceTop10 set successRate = ? ,cnt = ? where province = ?"
            statement = connection.prepareStatement(sql)
            statement.setString(1, elem._3)
            statement.setInt(2, elem._2)
            statement.setString(3, elem._1)
            statement.executeUpdate()
          } else {
            sql = "insert into provinceTop10(province,successRate,cnt) values(?,?,?)"
            statement = connection.prepareStatement(sql)
            statement.setString(1, elem._1)
            statement.setString(2, elem._3)
            statement.setInt(3, elem._2)
            statement.executeUpdate()
          }
        }
      } catch {
        case exception: Exception => exception.printStackTrace()
      }
    })
  }


  def perHourMappingFunction(key: String, value: Option[(Int, Double)], state: State[(Int, Double)]): (String, (Int, Double)) = {
    val size = value.getOrElse((0, 0.0))._1 + state.getOption().getOrElse((0, 0.0))._1
    val money = value.getOrElse((0, 0.0))._2 + state.getOption().getOrElse((0, 0.0))._2
    state.update((size, money))
    (key, (size, money))
  }

  def perHourRecharge(jsonDStream: DStream[JSONObject]) = {
    jsonDStream.filter(x => x.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq") &&
      x.getString("interFacRst").equals("0000") && x.getString("bussinessRst").equals("0000"))
      .mapPartitions(partition => {
        partition.map(json => {
          val hour = json.getString("requestId").substring(0, 10)
          val money = json.getString("chargefee").toDouble
          (hour, money)
        })
      }).groupByKey(24).map(item => {
      val size = item._2.size
      val money = item._2.reduce(_ + _)
      (item._1, (size, money))
    }).mapWithState(StateSpec.function(perHourMappingFunction _)).foreachRDD(rdd => {
      val top10 = rdd.collect()
      val connection = C3p0Util.getConnection
      for (elem <- top10) {
        var sql = "select count(*) cnt from recharge_hour where hour = ?"
        var statement = connection.prepareStatement(sql)
        statement.setString(1, elem._1)
        val resultSet = statement.executeQuery()
        var cnt = -1
        if (resultSet.next()) {
          cnt = resultSet.getInt("cnt")
        }
        if (cnt > 0) {
          sql = "update recharge_hour set cnt = ?, money = ? where hour = ?"
          statement = connection.prepareStatement(sql)
          statement = connection.prepareStatement(sql)
          statement.setInt(1, elem._2._1)
          statement.setDouble(2, elem._2._2)
          statement.setString(3, elem._1)
          statement.executeUpdate()
        } else {
          sql = "insert into recharge_hour(hour,cnt,money) values(?,?,?)"
          statement = connection.prepareStatement(sql)
          statement.setString(1, elem._1)
          statement.setInt(2, elem._2._1)
          statement.setDouble(3, elem._2._2)
          statement.executeUpdate()
        }
      }
    })
  }


}
