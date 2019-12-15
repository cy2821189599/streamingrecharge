package util

import java.util

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

object JedisClusterUtil {
  val config = new JedisPoolConfig()
  config.setMaxTotal(20);
  config.setMaxIdle(10);
  config.setMinIdle(5); //设置最小空闲数
  config.setMaxWaitMillis(10000);
  config.setTestOnBorrow(true);
  config.setTestOnReturn(true);
  //Idle时进行连接扫描
  config.setTestWhileIdle(true);
  //表示idle object evitor两次扫描之间要sleep的毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000);
  //表示idle object evitor每次扫描的最多的对象数
  config.setNumTestsPerEvictionRun(10);
  //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
  config.setMinEvictableIdleTimeMillis(60000);

  private val requirePass = PropertiesUtil.getProperty("requirePass", "jedisConfig.properties")
  private val connectTimeout = PropertiesUtil.getProperty("connectionTimeout", "jedisConfig.properties").toInt
  private val soTimeout = PropertiesUtil.getProperty("soTimeout", "jedisConfig.properties").toInt
  private val maxAttempts = PropertiesUtil.getProperty("maxAttempts", "jedisConfig.properties").toInt

  private val nodeSet: util.LinkedHashSet[HostAndPort] = new util.LinkedHashSet

  private val jediesStr: String = PropertiesUtil.getProperty("servers", "jedisConfig.properties")


  private val nodeStr = jediesStr.split(",")

  for (str <- nodeStr) {
    val fields = str.split(":")
    nodeSet.add(new HostAndPort(fields(0), fields(1).toInt))
  }

  private var cluster = new JedisCluster(nodeSet, connectTimeout, soTimeout, maxAttempts, requirePass, config)

  def getCluster(): JedisCluster = {
    if (cluster == null)
      cluster = new JedisCluster(nodeSet)
    cluster
  }

  /**
   * 获取之指定key 的value
   *
   * @param key 标记名
   * @return value default = false
   */
  def getValue(key: String) = {
    var v: String = "false"
    try {
      if (!cluster.exists(key)) {
        cluster.set(key, "false")
      } else {
        v = cluster.get(key)
      }
      v
    } catch {
      case ex: Exception => {
        cluster.close()
        println(ex.printStackTrace())
      }
    }
    v
  }

  /**
   * 设置指定key的value
   *
   * @param key
   * @param value
   */
  def setValue(key: String, value: String): Unit = {
    try {
      cluster.set(key, value)
    } catch {
      case ex: Exception => {
        cluster.close()
        println(ex.printStackTrace())
      }
    }
  }


}
