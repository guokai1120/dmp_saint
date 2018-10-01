package com.saintsup.config

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc.config.DBs

/**
  * 读取配置文件
  * Author by Kaka
  * Created on 2018/9/28 20:16
  */
object ConfigHandler {

  /**
    * 启动scalLikeJDBC读取配置文件
    */
    DBs.setup()
   private lazy val config: Config = ConfigFactory.load()
   /**
     * 获取数据路径
     */
  val dataInput: String = config.getString("data.input")
   /**
     * 写出数据的路径
     */
  val outputPath: String = config.getString("data.output")

  /**
    * 数据的序列化方式
    */
  val serializer: String = config.getString("spark.serializer")

  /**
    * 获取数据的相关设置
    */
  val driver: String = config.getString("db.default.driver")
  val url: String =config.getString("db.default.url")
  val user: String =config.getString("db.default.user")
  val password: String =config.getString("db.default.password")
  val t_table: String =config.getString("db.table")
  val t_table_area: String =config.getString("db.table_area")

}
