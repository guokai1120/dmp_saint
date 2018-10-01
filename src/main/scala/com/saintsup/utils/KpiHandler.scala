package com.saintsup.utils

import com.saintsup.beans._
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Author by Kaka
  * Created on 2018/9/30 21:57
  */
object KpiHandler {


  /**
    * 需要处理的数据
    * @param row
    * @return
    */
 def KpiUtils(row: Row) = {

    //原始请求 有效请求 广告请求 参与竞价数 竞价成功数 广告成本 广告消费 展示量 点击量

    val reqmode: Integer = row.getAs[Integer]("requestmode")
    val pronode: Integer = row.getAs[Integer]("processnode")
    /**
      * 是否有效
      */
    val iseffective: Integer = row.getAs[Integer]("iseffective")
    /**
      * 是否收费
      */
    val isbilling: Integer = row.getAs[Integer]("isbilling")
    /**
      * 是否rtp
      */
    val isbid: Integer = row.getAs[Integer]("isbid")
    /**
      * 是否竞价成功
      */
    val iswin: Integer = row.getAs[Integer]("iswin")
    /**
      * 广告的id
      */
    val adorderid: Integer = row.getAs[Integer]("adorderid")
    /**
      * 竞价成功的价格
      */
    val winprice: Double = row.getAs[Double]("winprice")
    /**
      * 转换后的广告消费
      */
    val adpayment: Double = row.getAs[Double]("adpayment")

    // 原始请求 有效请求 广告请求
    val reqAd: List[Double] = if (reqmode == 1 && pronode == 1) {
      List[Double](1, 0, 0)
    } else if (reqmode == 1 && pronode == 2) {
      List[Double](1, 1, 0)
    } else if (reqmode == 1 && pronode == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }

    // 参与竞价数 竞价成功数 广告成本 广告消费

    val priceAd: List[Double] = if (iseffective == 1 && isbid == 1 && isbilling == 1 && adorderid != 1) {
      List[Double](1, 0, 0, 0)
    } else if (iseffective == 1 && isbilling == 1 && iswin == 1) {
      List[Double](1, 1, winprice / 1000, adpayment / 1000)
    } else List[Double](0, 0, 0, 0)

    //展示量 点击量
    val countAd = if (iseffective == 1 && reqmode == 2) {
      List[Double](1, 0)
    } else if (iseffective == 1 && reqmode == 3) {
      List[Double](0, 1)
    } else List[Double](0, 0)
     reqAd ++ priceAd ++ countAd
  }

  /**
    * 地区分布的统计
    */
  def AreaAnalysis(linesRDD: DataFrame,ssc:SQLContext) = {
    /**
      * 数据分析
      */
    val resultRdd: RDD[((String, String), List[Double])] = linesRDD.map(row => {


      val adList: List[Double] = KpiHandler.KpiUtils(row)

      /**
        * key 统计维度字段
        * 1.省份
        * 2.城市
        */
      val pname: String = row.getAs[String]("provincename")
      val cname: String = row.getAs[String]("cityname")
      ((pname, cname), adList)
    })
    import ssc.implicits._
    val resultDF: DataFrame = resultRdd.reduceByKey((list1, list2) => {
      list1 zip list2 map (tp => tp._1 + tp._2)
    }).map(tp => RptAreaAnalysis(tp._1._1, tp._1._2,
      tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4),
      tp._2(5), tp._2(6), tp._2(7), tp._2(8))).toDF()
    resultDF
  }

  /**
    * 运营商的统计
    */

  def mobileAnalysis(linesRDD: DataFrame,ssc:SQLContext) = {
    /**
      * 数据分析
      */
    val resultRdd: RDD[(String, List[Double])] = linesRDD.map(row => {


      val adList: List[Double] = KpiHandler.KpiUtils(row)

      /**
        * key 统计维度字段
        */
      var ispname: String = ""

      if(ispname != "未知"){
        ispname = row.getAs[String]("ispname")
      }else{
        ispname = "其他"
      }
      (ispname, adList)
    })
    import ssc.implicits._
    val resultDF: DataFrame = resultRdd.reduceByKey((list1, list2) => {
      list1 zip list2 map (tp => tp._1 + tp._2)
    }).map(tp => RtpIspnameAnalysis(tp._1,
      tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4),
      tp._2(5), tp._2(6), tp._2(7), tp._2(8))).toDF()

    resultDF
  }
  /**
    * 网络类型的统计
    */

  def networkmannernameAnalysis(linesRDD: DataFrame,ssc:SQLContext) = {
    /**
      * 数据分析
      */
    val resultRdd: RDD[(String, List[Double])] = linesRDD.map(row => {


      val adList: List[Double] = KpiHandler.KpiUtils(row)

      /**
        * key 统计维度字段
        * 1.省份
        * 2.城市
        */
      var networkmannername: String = row.getAs[String]("networkmannername");

      if(networkmannername != "未知"){
      }else{
        networkmannername = "其他"
      }
      (networkmannername, adList)
    })
    import ssc.implicits._
    val resultDF: DataFrame = resultRdd.reduceByKey((list1, list2) => {
      list1 zip list2 map (tp => tp._1 + tp._2)
    }).map(tp => RtpNteworknameAnalysis(tp._1,
      tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4),
      tp._2(5), tp._2(6), tp._2(7), tp._2(8))).toDF()

    resultDF
  }
  /**
    * 设备类型的统计 core实现方式
    */

  def deviceNameAnalysis(linesRDD: DataFrame,ssc:SQLContext) = {
    /**
      * 数据分析
      */
    val resultRdd: RDD[(String, List[Double])] = linesRDD.map(row => {


      val adList: List[Double] = KpiHandler.KpiUtils(row)

      /**
        * key 统计维度字段
        * 设备类型
        */
      var deviceType: Int = row.getAs[Int]("devicetype")

      var devicename = ""
      if(deviceType == 1){
        devicename = "手机"
      }else if(deviceType == 2){
         devicename = "平板"
      }else devicename = "其他"
      (devicename, adList)
    })
    import ssc.implicits._
    val resultDF: DataFrame = resultRdd.reduceByKey((list1, list2) => {
      list1 zip list2 map (tp => tp._1 + tp._2)
    }).map(tp => RtpDevicenameAnalysis(tp._1,
      tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4),
      tp._2(5), tp._2(6), tp._2(7), tp._2(8))).toDF()

    resultDF
  }

  /**
    * 地区分布的统计
    */
  def appAnalysis(linesRDD: DataFrame,ssc:SQLContext,mapData:Broadcast[collection.Map[String, String]])= {
    /**
      * 数据分析
      */
    val resultRdd: RDD[(String, List[Double])] = linesRDD.map(row => {


      val adList: List[Double] = KpiHandler.KpiUtils(row)

      /**
        * key 统计维度字段
        * appname
        */
      var appname: String = row.getAs[String]("appname")
      val appid: String = row.getAs[String]("appid")

      /**
        * 判断appname是否为空 如果为空则根基appid去对应的表中查找
        */

      if(StringUtils.isEmpty(appname)){
        if(StringUtils.isNotEmpty(appid)){
          appname = mapData.value.getOrElse(appid,appid)
        }else{
          appname = "未知"
        }
      }
      (appname, adList)
    })
    import ssc.implicits._
    val resultDF: DataFrame = resultRdd.reduceByKey((list1, list2) => {
      list1 zip list2 map (tp => tp._1 + tp._2)
    }).map(tp => RptappAnalysis( tp._1,
      tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4),
      tp._2(5), tp._2(6), tp._2(7), tp._2(8))).toDF()
    resultDF
  }

}
