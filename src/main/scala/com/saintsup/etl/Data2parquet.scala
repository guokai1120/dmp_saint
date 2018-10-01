package com.saintsup.etl

import com.saintsup.beans.Logs
import com.saintsup.config.ConfigHandler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据转换为parquet文件
  * Author by Kaka
  * Created on 2018/9/29 21:43
  */
object Data2parquet {

  def main(args: Array[String]): Unit = {
    /**
      * sparkConf
      */
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer",ConfigHandler.serializer)

      /**
        * 设置压缩方式 优先级高于sparksql
        */
      .set("spark.sql.parquet.compression.codec", "snappy")
    /**
      * 创建sparkContext
      */
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    /**
      * 创建SQLContext
      */
    val ssc: SQLContext = new SQLContext(sc)

    /**
      * 这里可以设置压缩方式
      * 如果在sparkconf中设置 优先级要搞与在这里设置
      */
//    ssc.setConf("spark.sql.parquet.compression.codec", "snappy")

    /**
      * 读取数据
      */
    val lines: RDD[String] = sc.textFile(ConfigHandler.dataInput)
    val splitsRdd: RDD[Array[String]] = lines.map(_.split(",",-1)).filter(_.length >= 85)

    /**
      * 处理数据
      */

    val logRdd: RDD[Logs] = splitsRdd.map(arr=>Logs(arr))

    val dataFrame = ssc.createDataFrame(logRdd)

    dataFrame.coalesce(1).write.parquet(ConfigHandler.outputPath)

    /***
      * 释放资源
      */
    sc.stop()
  }

}
