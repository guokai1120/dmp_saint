package com.saintsup.utils

import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import java.security.NoSuchAlgorithmException

import com.alibaba.fastjson.{JSON, JSONArray}
import com.saintsup.config.ConfigHandler
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.util.Random

/**
  * 根据经纬度获取地区
  * Author by Kaka
  * Created on 2018/9/28 10:27
  */

object GetBussinessByLatAndLong {

  def main(args: Array[String]): Unit = {
    val url = parseJson("39.814435","116.467523")
    println(url.toString)
  }

  /**
    * 解析经纬度生成的json数据
    * @param lat
    * @param log
    */
  def parseJson(lat:String,log:String)={
    /**
      * 获取Json数据
      */
     val jsonString: String = getJson(lat,log)
    var business = new StringBuffer()
    /**
      * 判断是否为空
      */
    if(StringUtils.isNotEmpty(jsonString)){
      val jSONObject = JSON.parseObject(jsonString)
      val status = jSONObject.getInteger("status")
      if(status==0){
        val nObject = jSONObject.getJSONObject("result")
        /**
          * 商圈信息
          */
        business = business.append(nObject.getString("business"))

        if(StringUtils.isEmpty(business.toString)){
          //如果business为空 继续获取 pois
          val poisArr: JSONArray = nObject.getJSONArray("pois")
          for(i <- 0 until poisArr.size()){
            val tag: String = poisArr.getJSONObject(i).getString("tag")
            if(StringUtils.isNotEmpty(tag))  business = business.append(tag)
          }
        }

      }
    }
    business
  }
  /**
    * 根据经纬度获取地区
    * @param lat
    * @param log
    */
  private  def getJson(lat:String,log:String)={
    /**
      * 获取url
      */
    val url = getUrl(lat,log)
    /**
      * 创建一个client
      */
    val client = new HttpClient()
    val method = new GetMethod(url)
    val code = client.executeMethod(method)
    if(code==200){
      val jsonString = method.getResponseBodyAsString
     jsonString
    }else{
      ""
    }
  }
  @throws[UnsupportedEncodingException]
  @throws[NoSuchAlgorithmException]
  private def getUrl(lat:String,log:String)= {
    // 计算sn跟参数对出现顺序有关，
    // get请求请使用LinkedHashMap保存<key,value>，
    // 该方法根据key的插入顺序排序；post请使用TreeMap保存<key,value>，
    // 该方法会自动将key按照字母a-z顺序排序。
    // 所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，
    // 但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。
    // 以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，paramsMap中先放入address，
    // 再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
    /**
      * 逆地理編碼
      * http://api.map.baidu.com/geocoder/v2/?callback=renderReverse&location=35.658651,139.745415&output=json&pois=1&ak=您的ak //GET请求
      */
     val index = Random.nextInt(ConfigHandler.baiduAsk.values().size())
    val keySet = ConfigHandler.baiduAsk.keySet().toArray
    val paramsMap = new mutable.LinkedHashMap[String,String]
    paramsMap.put("location", lat+","+log)
    paramsMap.put("output", "json")
    paramsMap.put("pois", "1")
    paramsMap.put("ak", keySet(index).asInstanceOf[String])
    // 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
    val paramsStr = toQueryString(paramsMap)
    val wholeStr = new String("/geocoder/v2/?" + paramsStr + ConfigHandler.baiduAsk.get(keySet(index)))
    val tempStr = URLEncoder.encode(wholeStr, "UTF-8")
    // 调用下面的MD5方法得到最后的sn签名7de5a22212ffaa9e326444c75a58f9a0
    val sn = MD5(tempStr)

  ConfigHandler.base +paramsStr +"&sn="+sn
  }

  // 对Map内所有value作utf8编码，拼接返回结果
  @throws[UnsupportedEncodingException]
  private def toQueryString(data: mutable.LinkedHashMap[String,String]): String = {
    val queryString = new StringBuffer
    import scala.collection.JavaConversions._
    for (pair <- data.entrySet) {
      queryString.append(pair.getKey + "=")
      queryString.append(URLEncoder.encode(pair.getValue, "UTF-8") + "&")
    }
    if (queryString.length > 0) queryString.deleteCharAt(queryString.length - 1)
    queryString.toString
  }

  // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
  private def MD5(md5: String): String = {
    try {
      val md = java.security.MessageDigest.getInstance("MD5")
      val array = md.digest(md5.getBytes)
      val sb = new StringBuffer
      var i = 0
      while ( {
        i < array.length
      }) {
        sb.append(Integer.toHexString((array(i) & 0xFF) | 0x100).substring(1, 3))

        {
          i += 1; i
        }
      }
      return sb.toString
    } catch {
      case e: NoSuchAlgorithmException =>

    }
    null
  }
}
