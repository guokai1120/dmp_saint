package com.saintsup.report

import com.saintsup.config.ConfigHandler
import com.saintsup.utils.{DBUtils, KpiHandler}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域分布的数据分析
  * Author by Kaka
  * Created on 2018/9/30 20:16
  */
object RtpAreaAnalysissql {
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

    ssc.udf.register("myif",(boolean:Boolean)=>(if (boolean) 1 else 0))

    /**
      * 处理数据 SQL方式
      */

    //requestmode processnode iseffective isbilling isbid iswin
    //adorderid  winprice adpayment
    linesRDD.registerTempTable("log")
    val result: DataFrame = ssc.sql(
      """
         |select case devicetype
         |when 1  then "手机"
         |when 2  then "平板"
         |else "其他"
         |end devicename,
         |sum(myif(requestmode=1 and processnode>=1)) adRawReq,
         |sum(if(requestmode=1 and processnode>=2, 1, 0)) adEffReq,
         |sum(if(requestmode=1 and processnode=3, 1, 0)) adReq,
         |
         |sum(if(iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0, 1, 0)) adRtbReq,
         |sum(if(iseffective=1 and isbilling=1 and iswin=1, 1, 0)) adWinReq,
         |
         |sum(if(requestmode=2 and iseffective=1, 1, 0)) adShow,
         |sum(if(requestmode=3 and iseffective=1, 1, 0)) adClick,
         |
         |sum(if(iseffective=1 and isbilling=1 and iswin=1, winprice/1000, 0)) winprice,
         |sum(if(iseffective=1 and isbilling=1 and iswin=1, adpayment/1000, 0)) adpayment
         |
         |from log group by devicetype
      """.stripMargin
    )

    result.foreach(println)
    /**
      * 设备类型的统计
      */
//    val deviceNameResultDF = KpiHandler.deviceNameAnalysis(linesRDD,ssc)

    /**
      * 写入数据库
      */
//    DBUtils.write2DB(result,ConfigHandler.url,ConfigHandler.t_table+"devicenamesql",ConfigHandler.driver,ConfigHandler.user,ConfigHandler.password)



    /**
      * 释放资源
      */
    sc.stop()
  }
}
