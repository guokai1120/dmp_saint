package com.saintsup.graph

import com.saintsup.Tags.{GetUserIdLabel, Tag4Bussiness, TagKeyword4User}
import com.saintsup.config.ConfigHandler
import com.saintsup.utils.Jpools
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用graph聚合相同用户的标签
  * Author by Kaka
  * Created on 2018/10/4 19:01
  */
object Graph4UserId {

  def main(args: Array[String]): Unit = {

    /**
      * 创建sparkConf
      * 创建sparkContext
      */
    val conf: SparkConf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[*]")
      .set("key.serializer",ConfigHandler.serializer)
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    /**
      * 创建SQLContext
      */
    val ssc: SQLContext = new SQLContext(sc)

    /**
      * 去取文件
      */
    val linesDF: DataFrame = ssc.read.parquet(ConfigHandler.outputPath)

    /**
      * 读取app_Dict文件
      */
    val appData: collection.Map[String, String] = sc.textFile(ConfigHandler.appdictPath)
      .map(line => line.split(",", -1))
      .filter(_.length > 5)
      .map(arr => {
        (arr(4), arr(0))
      })
      .collectAsMap()
    /**
      * 读取stopword文件
      */
    val stopwordData: collection.Map[String, String] = sc.textFile(ConfigHandler.stopwordpath)
      .map((_,null))
      .collectAsMap()
    /**
      * 将数据广播出去
      */
    val stopwordBT: Broadcast[collection.Map[String, String]] = sc.broadcast(stopwordData)
    val appDT: Broadcast[collection.Map[String, String]] = sc.broadcast(appData)
    /**
      * 构建集合
      * 顶点的集合
      */
    val vertices: RDD[(Long, List[(String, Int)])] = linesDF.filter(GetUserIdLabel.hasOneUserId)
      .mapPartitions(partion => {
        Class.forName("java.sql.Driver")
        val jedis = Jpools.getCon()
        val partionResult = partion.flatMap(row => {
          /**
            * 存储数据的List
            */
          var list: List[(String, Int)] = List[(String, Int)]()

          // 广告位类型（标签格式： LC03->1 或者LC16->1）xx 为数字，小于10 补0
          val adType: Int = row.getAs[Int]("adspacetype")
          val adname: String = row.getAs[String]("adspacetypename")
          if (adType > 10) list :+= ("LC" + adType, 1) else if (adType >= 0) list :+= ("LC0" + adType, 1)
          if (StringUtils.isNotEmpty(adname)) list :+= ("LN" + adname, 1)

          /** App 名称（标签格式： APPxxxx->1）xxxx 为App 名称，
            * 使用缓存文件appname_dict进行名称转换
            */
          var appname: String = row.getAs[String]("appname")
          val appid: String = row.getAs[String]("appid")
          if (StringUtils.isEmpty(appname)) {
            if (StringUtils.isNotEmpty(appid)) list :+= ("APP" + appDT.value.getOrElse(appid, appid), 1)
          } else list :+= ("APP" + appname, 1)

          /**
            * keyword
            */
          val keywordList = TagKeyword4User.getLabel4User(row, stopwordBT)
          /**
            * 渠道
            * 渠道（标签格式： CNxxxx->1）xxxx 为渠道ID
            */
          val channel: Int = row.getAs[Int]("adplatformproviderid")
          if (channel > 0) list :+= ("CN" + channel, 1)

          //设备类型
          val client: Int = row.getAs[Int]("client")
          client match {
            case 1 => list :+= ("D00010001", 1)
            case 2 => list :+= ("D00010002", 1)
            case 3 => list :+= ("D00010003", 1)
            case _ => list :+= ("D00010004", 1)
          }
          //联网方式
          val network: String = row.getAs[String]("networkmannername")
          network.toUpperCase match {
            case "WIFI" => list :+= ("D00020001", 1)
            case "4G" => list :+= ("D00020002", 1)
            case "3G" => list :+= ("D00020003", 1)
            case "2G" => list :+= ("D00020004", 1)
            case _ => list :+= ("D00020005", 1)
          }
          // 运营商名称
          val ispname: String = row.getAs[String]("ispname")
          ispname match {
            case "移动" => list :+= ("D00030001", 1)
            case "电信" => list :+= ("D00030002", 1)
            case "联通" => list :+= ("D00030003", 1)
            case _ => list :+= ("D00030004", 1)
          }

          /**
            * 地域
            * 地域标签（省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）xxx 为省或市名称
            */
          val pname: String = row.getAs[String]("provincename")
          val cname: String = row.getAs[String]("cityname")
          if (StringUtils.isNotEmpty(pname)) list :+= ("ZP" + pname, 1)
          if (StringUtils.isNotEmpty(cname)) list :+= ("ZC" + cname, 1)
          /**
            * 商圈标签
            */
          val business: List[(String, Int)] = Tag4Bussiness.getLabel4User(row, jedis)
          /**
            * 用户id 获取十五个字段中有值得哪一个
            */

          val allUserIds: ListBuffer[String] = GetUserIdLabel.getAllUserIds(row)

          val userIdsList: ListBuffer[(String, Int)] = allUserIds.map((_, 0))

          val currentLineTags: List[(String, Int)] = list ++ keywordList ++ list ++ business ++ userIdsList

          allUserIds.map(uId => {
            if (uId.equals(allUserIds.head)) (uId.hashCode.toLong, currentLineTags)
            else (uId.hashCode.toLong, Nil)
          })
        })
        jedis.close()
        partionResult
      })
      .reduceByKey((list1,list2)=>{
        (list1 ++ list2).groupBy(_._1)
          /**
            * 三种实现方式
            */
          //          .mapValues(value=>value.length).toList
          //          .mapValues(value=>value.map(_._2).sum).toList
          .mapValues(value=>value.foldLeft(0)(_+_._2)).toList
      })

    /**
      * 创建边的集合
      */
    val edges: RDD[Edge[Int]] = linesDF.flatMap(line => {
      val allUserId = GetUserIdLabel.getAllUserIds(line)

      allUserId.map(uId => Edge(allUserId.head.hashCode.toLong, uId.hashCode.toLong, 0))
    })

    /**
      * 获取graph对象
      */
    val graph: Graph[List[(String, Int)], Int] = Graph(vertices ,edges)
    val cc = graph.connectedComponents().vertices

    val resultRdd: RDD[(VertexId, List[(String, Int)])] = cc.join(vertices).map {
      case (uid, (mindId, tagsAndUserId)) => (mindId, tagsAndUserId)
    }.reduceByKey {
      case (list1, list2) => (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }

    /**
      * 将数据合并
      */

    /**
      * 判断文件是否存在
      */
    val hadoopConf = sc.hadoopConfiguration

    val fs = FileSystem.get(hadoopConf)

    val path = new Path("e:/data/lablePath")

    if(fs.exists(path)){
      fs.delete(path,true)
    }
    /**
      * 将数据写出文件
      */
    resultRdd.saveAsTextFile("e:/data/graphData")
    /**
      * 释放资源
      */
    sc.stop()
  }
}
