package com.saintsup.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Author by Kaka
  * Created on 2018/10/1 15:53
  */
object Jpools {

  /**
    * 创建一个配置文件
    */
  private val config = new GenericObjectPoolConfig()
  /**
    * 初始化一个redis连接池
    */
  private lazy val jpool = new JedisPool("master",6379)
  private lazy val pool: JedisPool = new JedisPool(config,"master")

  /**
    * 设置index的默认值的方式
    * @param index
    * @return
    */
  def getCon(index:Int = 0)={

    val jedis = pool.getResource
    jedis.select(index)
    jedis
  }
}
