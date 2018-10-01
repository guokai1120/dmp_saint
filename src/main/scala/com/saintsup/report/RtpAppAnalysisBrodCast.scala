package com.saintsup.report

import com.saintsup.config.ConfigHandler
import com.saintsup.utils.{DBUtils, KpiHandler, MediaDataUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域分布的数据分析
  * Author by Kaka
  * Created on 2018/9/30 20:16
  */
object RtpAppAnalysisBrodCast{
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
    val broadCastData: RDD[(String, String)] = MediaDataUtils.getBroadCastData(sc)

    val mapData: collection.Map[String, String] = broadCastData.collectAsMap()

    val broadcastMap: Broadcast[collection.Map[String, String]] = sc.broadcast(mapData)

    val resultDF: DataFrame = KpiHandler.appAnalysis(linesRDD,ssc,broadcastMap)

    resultDF.foreach(println)

    DBUtils.write2DB(resultDF,ConfigHandler.url,ConfigHandler.t_table+"appname",ConfigHandler.driver,ConfigHandler.user,ConfigHandler.password)

    /**
      * 释放资源
      */
    sc.stop()
  }
}
