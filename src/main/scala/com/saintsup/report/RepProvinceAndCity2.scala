package com.saintsup.report

import com.saintsup.config.ConfigHandler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scalikejdbc.{DB, SQL}

/**
  * 统计省市的数量 sparkcore的实现形式
  * Author by Kaka
  * Created on 2018/9/30 12:34
  */
object RepProvinceAndCity2 {

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
      * core实现
      */
    val resultRdd: RDD[((String, String), Int)] = linesRDD.map(row => {
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
      ((provincename, cityname), 1)
    }).reduceByKey(_ + _)
    /**
      * 写到本地磁盘
      * 方式一借助于样例类
      */
  /* val classRdd: RDD[provinceAndCity] = resultRdd.map(tp=>provinceAndCity(tp._1._1,tp._1._2,tp._2))

    val resultRDD: DataFrame = ssc.createDataFrame(classRdd)

    resultRDD.coalesce(1).write.json("e:/data/outputcore")*/

    /**
      * 方式2 直接将样例类转换为Json
      */
//    val gsonRdd: RDD[String] = resultRdd.map(tp => {
//      val gson = new Gson()
//      gson.toJson(provinceAndCity(tp._1._1, tp._1._2, tp._2))
//    })
//
//    gsonRdd.saveAsTextFile("e:/data/gsonoutput")
    /**
      * 当时三 toDF
      */
//    import ssc.implicits._
//    val resultRddDataFrame = resultRdd.map(tp => (tp._1._1, tp._1._2, tp._2))
//      .toDF("provincename", "cityname", "cnt")


//    resultRddDataFrame.write.json("e:/data/todfoutput1")
    /**
      * 写入数据库 方式一
      */
//    val props = new Properties()
//    props.setProperty("driver",ConfigHandler.driver)
//    props.setProperty("user",ConfigHandler.user)
//    props.setProperty("password",ConfigHandler.password)
//    resultRddDataFrame.write.jdbc(ConfigHandler.url,ConfigHandler.t_table + "2",props)

    /**
      * 写入数据库的方式二
      * scalLikeJDBC
      */
    resultRdd.foreachPartition(it=>{
      DB.localTx(implicit session=>{
        it.foreach(tp=>{
          SQL("insert into t_province_city_scalLikeJDBC(provincename,cityname,cnt) values(?,?,?)")
            .bind(tp._1._1,tp._1._2,tp._2).update().apply()
        })
      })
    })

    /**
      * 释放资源
      */
    sc.stop()
  }
}
case class provinceAndCity(provincename:String,cityname:String,cnt:Int)