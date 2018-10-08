package com.saintsup.Tags

import com.saintsup.Tags.Tag4Userid.getClass
import com.saintsup.config.ConfigHandler
import com.saintsup.utils.{DBUtils, GetBussinessByLatAndLong, doLatAndLng2GeoHash}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 将经纬度 对应的商圈存入数据库
  * Author by Kaka
  * Created on 2018/10/3 14:37
  */
object Business2BD {

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

    import  ssc.implicits._
    val resulDF: DataFrame = linesDF.select("lat", "long")
      .filter("long > 65 and long < 135 and lat <= 55 and lat > 15")
      .distinct()
      .map(row => {
        val lat: String = row.getAs[String]("lat")
        val lng: String = row.getAs[String]("long")

        /**
          * 获取GeoHash编码
          */
        val geoHashString: String = doLatAndLng2GeoHash.latAndLngToGeoHash(lat, lng, 12)

        /**
          * 获取商圈信息
          */
        val businessStr: String = GetBussinessByLatAndLong.parseJson(lat, lng).toString
        (geoHashString, businessStr)
      }).toDF("geoHash", "bussiness")

    /**
      * 写入数据库
      */
    DBUtils.write2DB(resulDF,ConfigHandler.url,ConfigHandler.t_table+"_geoHash_business",ConfigHandler.driver,ConfigHandler.user,ConfigHandler.password)

    /**
      * 释放资源
      */
    sc.stop()
  }
}
