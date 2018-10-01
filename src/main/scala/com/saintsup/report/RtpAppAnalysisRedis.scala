package com.saintsup.report

import com.saintsup.config.ConfigHandler
import com.saintsup.utils.{DBUtils, Jpools, KpiHandler}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域分布的数据分析
  * Author by Kaka
  * Created on 2018/9/30 20:16
  */
object RtpAppAnalysisRedis{
  def main(args: Array[String]): Unit = {

    /**
      * 创建sparkContext
      * 创建sparkConf
      */
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer",ConfigHandler.serializer)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    /**
      * 创建sqlContext
      */
    val ssc = new SQLContext(sc)

    /**
      * 读取数据
      */
    val linesRDD: DataFrame = ssc.read.parquet(ConfigHandler.outputPath)

    /**
      * 媒体数据分析处理
      */

    val resultRdd: RDD[(String, List[Double])] = linesRDD.mapPartitions(ite => {
      val jedis = Jpools.getCon()
      val iterAnaylsis = ite.map(row => {
        var appname: String = row.getAs[String]("appname")
        val appid: String = row.getAs[String]("appid")
        if (StringUtils.isEmpty(appname)) {
          if (StringUtils.isNotEmpty(appid)) {
            appname = jedis.get(appid)
            if (StringUtils.isEmpty(appname)) {
              appname = appid;
            }
          } else {
            appname = "未知"
          }
        }
        (appname, KpiHandler.KpiUtils(row))
      })
      jedis.close()
      iterAnaylsis
    }).reduceByKey((list1,list2)=>list1 zip list2 map(tp=>tp._1 + tp._2))

    import ssc.implicits._
    val resultDF: DataFrame = resultRdd.map(tp => (tp._1,
      tp._2(0), tp._2(1), tp._2(2),
      tp._2(4), tp._2(5), tp._2(6),
      tp._2(7), tp._2(8))).toDF(
      "appName","adRawReq", "adReffReq", "adReq", "adRTBReqe", "adShow", "adClick", "adCost", "adPay"
    )
    /**
      * 写入数据库
      */
    DBUtils.write2DB(resultDF,ConfigHandler.url,ConfigHandler.t_table+"appnameRedis",ConfigHandler.driver,ConfigHandler.user,ConfigHandler.password)

    /**
      * 释放资源
      */
    sc.stop()
  }
}
