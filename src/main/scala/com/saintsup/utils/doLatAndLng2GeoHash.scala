package com.saintsup.utils

import ch.hsr.geohash.GeoHash

/**
  * 将经纬度转换为geohash
  * Author by Kaka
  * Created on 2018/10/2 22:07
  */
object doLatAndLng2GeoHash {
  /**
    * 根据经纬度 转换为GeoHash字符串
    * @param lat 经度
    * @param lng 纬度
    * @param numberOfCharcater  保存的位数
    * @return 返回一个GeoHash字符串
    */
  def latAndLngToGeoHash(lat:String,lng:String,numberOfCharcater:Int)={

    /**
      * 首先获取一个geohash
      */
    val geohash: GeoHash = GeoHash.withCharacterPrecision(lat.toDouble,lng.toDouble,numberOfCharcater)

    /**
      * 转换为Base32String
      */
    val base32String: String = geohash.toBase32

    base32String

  }
}
