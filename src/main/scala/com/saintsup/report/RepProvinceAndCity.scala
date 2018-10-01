package com.saintsup.report

import java.util.Properties

import com.saintsup.config.ConfigHandler
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计省市的数量
  * Author by Kaka
  * Created on 2018/9/30 12:34
  */
object RepProvinceAndCity {

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
      * sql实现
      */
    linesRDD.registerTempTable("lineRdd")

    val result: DataFrame = ssc.sql("select provincename,cityname,count(*) as ct from lineRdd group by provincename,cityname")

    /**
      * 写到本地磁盘
      */
    result.coalesce(1).write.json("e:/data/ouputpath/")

    /**
      * 写入数据库
      */
    val props = new Properties()
    props.setProperty("driver",ConfigHandler.driver)
    props.setProperty("user",ConfigHandler.user)
    props.setProperty("password",ConfigHandler.password)
    result.write.jdbc(ConfigHandler.url,ConfigHandler.t_table,props)
    /**
      * 释放资源
      */
    sc.stop()
  }
}
