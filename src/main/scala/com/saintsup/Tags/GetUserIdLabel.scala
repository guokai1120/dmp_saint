package com.saintsup.Tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 获取用户的id
  * Author by Kaka
  * Created on 2018/10/2 20:13
  */
object GetUserIdLabel {
  /**
    * 保证至少有一个不为空才执行
    */
  val hasOneUserId =
    """
      |imei!="" or mac!="" or idfa!="" or androidid!="" or openudid!="" or
      |imeimd5!="" or macmd5!="" or idfamd5!="" or androididmd5!="" or openudidmd5!="" or
      |imeisha1!="" or macsha1!="" or idfasha1!="" or androididsha1!="" or openudidsha1!=""
    """.stripMargin
  /**
    * 获取一个uid 只要找到一个 其他的就不会执行 类似于else if
    * @param row
    * @return
    */
  def getUserId(row:Row)={
    row match {
      case row if StringUtils.isNotEmpty(row.getAs[String]("imei"))           => "IM#" + row.getAs[String]("imei")
      case row if StringUtils.isNotEmpty(row.getAs[String]("mac"))            => "MC#" + row.getAs[String]("mac")
      case row if StringUtils.isNotEmpty(row.getAs[String]("idfa"))           => "ID#" + row.getAs[String]("idfa")
      case row if StringUtils.isNotEmpty(row.getAs[String]("androidid"))      => "AD#" + row.getAs[String]("androidid")
      case row if StringUtils.isNotEmpty(row.getAs[String]("openudid"))       => "OU#" + row.getAs[String]("openudid")
      case row if StringUtils.isNotEmpty(row.getAs[String]("imeimd5"))        => "IMM#"+row.getAs[String]("imeimd5")
      case row if StringUtils.isNotEmpty(row.getAs[String]("macmd5"))         => "MCM#"+row.getAs[String]("macmd5")
      case row if StringUtils.isNotEmpty(row.getAs[String]("idfamd5"))        => "IDM#"+row.getAs[String]("idfamd5")
      case row if StringUtils.isNotEmpty(row.getAs[String]("androididmd5"))   => "ADM#"+row.getAs[String]("androididmd5")
      case row if StringUtils.isNotEmpty(row.getAs[String]("openudidmd5"))    => "OUM#"+row.getAs[String]("openudidmd5")
      case row if StringUtils.isNotEmpty(row.getAs[String]("imeisha1"))       => "IMS#"+row.getAs[String]("imeisha1")
      case row if StringUtils.isNotEmpty(row.getAs[String]("macsha1"))        => "MCS#"+row.getAs[String]("macsha1")
      case row if StringUtils.isNotEmpty(row.getAs[String]("idfasha1"))       => "IDS#"+row.getAs[String]("idfasha1")
      case row if StringUtils.isNotEmpty(row.getAs[String]("androididsha1"))  => "ADS#"+row.getAs[String]("androididsha1")
      case row if StringUtils.isNotEmpty(row.getAs[String]("openudidsha1"))   => "OUS#"+row.getAs[String]("openudidsha1")
    }
  }

  /**
    * 获取所有用户的ids
    * @param row
    * @return
    */
  def getAllUserIds(row:Row)= {

     val list = new ListBuffer[String]()

     if (StringUtils.isNotEmpty(row.getAs[String]("imei"))         ) list.append("IM#" + row.getAs[String]("imei"))
     if (StringUtils.isNotEmpty(row.getAs[String]("mac"))          ) list.append("MC#" + row.getAs[String]("mac"))
     if (StringUtils.isNotEmpty(row.getAs[String]("idfa"))         ) list.append("ID#" + row.getAs[String]("idfa"))
     if (StringUtils.isNotEmpty(row.getAs[String]("androidid"))    ) list.append("AD#" + row.getAs[String]("androidid"))
     if (StringUtils.isNotEmpty(row.getAs[String]("openudid"))     ) list.append("OU#" + row.getAs[String]("openudid"))
     if (StringUtils.isNotEmpty(row.getAs[String]("imeimd5"))      ) list.append("IMM#" + row.getAs[String]("imeimd5"))
     if (StringUtils.isNotEmpty(row.getAs[String]("macmd5"))       ) list.append("MCM#" + row.getAs[String]("macmd5"))
     if (StringUtils.isNotEmpty(row.getAs[String]("idfamd5"))      ) list.append("IDM#" + row.getAs[String]("idfamd5"))
     if (StringUtils.isNotEmpty(row.getAs[String]("androididmd5")) ) list.append("ADM#" + row.getAs[String]("androididmd5"))
     if (StringUtils.isNotEmpty(row.getAs[String]("openudidmd5"))  ) list.append("OUM#" + row.getAs[String]("openudidmd5"))
     if (StringUtils.isNotEmpty(row.getAs[String]("imeisha1"))     ) list.append("IMS#" + row.getAs[String]("imeisha1"))
     if (StringUtils.isNotEmpty(row.getAs[String]("macsha1"))      ) list.append("MCS#" + row.getAs[String]("macsha1"))
     if (StringUtils.isNotEmpty(row.getAs[String]("idfasha1"))     ) list.append("IDS#" + row.getAs[String]("idfasha1"))
     if (StringUtils.isNotEmpty(row.getAs[String]("androididsha1"))) list.append("ADS#" + row.getAs[String]("androididsha1"))
     if (StringUtils.isNotEmpty(row.getAs[String]("openudidsha1")) ) list.append("OUS#" + row.getAs[String]("openudidsha1"))

    list
  }
}
