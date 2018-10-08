package com.saintsup.Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * Author by Kaka
  * Created on 2018/10/2 20:39
  */
object TagKeyword4User  extends Tag4Others {
  /**
    * 实现不同维度标签
    *
    * @param args 传递参数 可以传递多个
    * @return 返回一个List[(String,Int)]
    */
  override def getLabel4User(args: Any*): List[(String, Int)] = {
    /**
      * 关键字
      * （标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于3 个字符，且不能
      * 超过8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
      */
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopwordBT = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    val keyword: String = row.getAs[String]("keywords")

    keyword.split("\\|")
      .filter(key => key.length > 3 && key.length < 8 && !stopwordBT.value.contains(key))
      .foreach(key => list :+= ("k" + key, 1))
    list
  }
}
