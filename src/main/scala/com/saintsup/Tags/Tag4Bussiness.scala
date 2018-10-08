package com.saintsup.Tags

import com.saintsup.utils.doLatAndLng2GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * Author by Kaka
  * Created on 2018/10/3 16:50
  */
object Tag4Bussiness extends Tag4Others {
  /**
    * 实现不同维度标签
    *
    * @param args 传递参数 可以传递多个
    * @return 返回一个List[(String,Int)]
    */
  override def getLabel4User(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val conn = args(1).asInstanceOf[Jedis]

    val lat: String = row.getAs[String]("lat")
    val lng: String = row.getAs[String]("long")
    import com.saintsup.utils.ImplictStr._
    if(lat.toDoublex > 15 && lat.toDoublex <= 55 && lng.toDoublex > 65 && lng.toDoublex <=135){
      val geoHash: String = doLatAndLng2GeoHash.latAndLngToGeoHash(lat,lng,12)
      list :+= ("GC"+geoHash,1)
      val bussiness: String = conn.get(geoHash)

      if(StringUtils.isNotEmpty(bussiness))  list :+= ("BN"+bussiness,1)
    }
    list
  }
}
