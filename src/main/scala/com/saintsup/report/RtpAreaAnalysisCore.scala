package com.saintsup.report

import java.util.Properties

import com.saintsup.config.ConfigHandler
import com.saintsup.utils.{DBUtils, KpiHandler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域分布的数据分析
  * Author by Kaka
  * Created on 2018/9/30 20:16
  */
object RtpAreaAnalysisCore {
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
    val linesRDD: DataFrame = ssc.read.parquet(ConfigHandler.outputPath).cache()

    /**
      * 地区的统计
      */
    val areaResultDF: DataFrame =KpiHandler.AreaAnalysis(linesRDD,ssc)

    /**
      * 运营商的统计
      */
    val mobleResultDF = KpiHandler.mobileAnalysis(linesRDD,ssc)

    /**
      * 网络类型的统计
      */
    val networknameResultDF = KpiHandler.networkmannernameAnalysis(linesRDD,ssc)

    /**
      * 设备类型的统计
      */
    val deviceNameResultDF = KpiHandler.deviceNameAnalysis(linesRDD,ssc)

    /**
      * 写入数据库
      */
    DBUtils.write2DB(deviceNameResultDF,ConfigHandler.url,ConfigHandler.t_table+"devicename",ConfigHandler.driver,ConfigHandler.user,ConfigHandler.password)



    /**
      * 释放资源
      */
    sc.stop()
  }
}
