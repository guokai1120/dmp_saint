package com.saintsup.tools

import com.saintsup.config.ConfigHandler
import com.saintsup.report.RtpAreaAnalysisCore.getClass
import com.saintsup.utils.Jpools
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Author by Kaka
  * Created on 2018/10/1 17:17
  */
object AppData2Redis {

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
    val lines: RDD[String] = sc.textFile("e:/data/app_dict.txt")

    val app_idRdd: RDD[(String, String)] = lines.map(_.split("\\t", -1))
      .filter(_.length >= 4)
      .map(arr => {(arr(4), arr(1))})

    app_idRdd.foreachPartition(ite=>{
      val jedis = Jpools.getCon()
      ite.foreach(tp=>{
        jedis.hset("apps",tp._1,tp._2)
      })
      jedis.close()
    })

    sc.stop()

  }
}
