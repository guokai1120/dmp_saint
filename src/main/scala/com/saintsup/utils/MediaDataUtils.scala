package com.saintsup.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author by Kaka
  * Created on 2018/10/1 15:45
  */
object MediaDataUtils {

  def getBroadCastData(sc:SparkContext) = {
    /**
      * 读取数据
      */
    val lines: RDD[String] = sc.textFile("e:/data/app_dict.txt")

    val app_idRdd: RDD[(String, String)] = lines.map(_.split("\\t", -1))
      .filter(_.length >= 4)
      .map(arr => {(arr(4), arr(1))})

    app_idRdd
  }
}
