package com.saintsup.utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * 写入数据库
  * Author by Kaka
  * Created on 2018/10/1 2:41
  */
object DBUtils {

  /**
    * 将数据写入数据库的方式
    * @param resultDF 需要写入数据可的DataFrame
    * @param url  数据路的路径
    * @param tableName 数据表名
    * @param driver 驱动
    * @param user 数据库用户名
    * @param password 数据库密码
    */
  def write2DB(resultDF:DataFrame,
               url:String,
               tableName:String,
               driver:String,
               user:String,
               password:String
              ): Unit ={
    /**
      * 写入数据库
      */
    val props = new Properties()
    props.setProperty("driver",driver)
    props.setProperty("user",user)
    props.setProperty("password",password)
    resultDF.write.mode(SaveMode.Overwrite).jdbc(url,tableName,props)
  }
}
