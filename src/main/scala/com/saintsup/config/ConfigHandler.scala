package com.saintsup.config

import java.util

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
  val appdictPath: String = config.getString("data.app.dict")
  val stopwordpath: String = config.getString("data.stopword")

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

  /**
    * 百度ak sk base
    */
  val ak: String = config.getString("baidu.ak")
  val sk: String = config.getString("baidu.sk")
  val base: String = config.getString("baidu.base")
  /**
    * 测试的经纬度
    */
  val lat: String = config.getString("baidu.lat")
  val log: String = config.getString("baidu.log")
  /**
    * 心知天气
    */
  val xzBase: String = config.getString("xinzhi.base")

  /**
    * 随机获取百度的ask
    */
  val baiduAsk: util.Map[String, AnyRef] = config.getObject("baidu.ask").unwrapped()
}
