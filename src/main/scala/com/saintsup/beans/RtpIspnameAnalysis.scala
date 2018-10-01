package com.saintsup.beans

/**
  * 样例类
  * Author by Kaka
  * Created on 2018/10/1 2:37
  */
/**
  * 运营商的统计
  */
case class RtpIspnameAnalysis(ispname:String,
                              adRawReq:Double,
                              adReffReq:Double,
                              adReq:Double,
                              adRTBReqe:Double,
                              adRTBSuccReq:Double,
                              adCost:Double,
                              adPay:Double,
                              adShow:Double,
                              adClick:Double
                             )
/**
  * 网络类型样例类
  */
case class RtpNteworknameAnalysis(ispname:String,
                              adRawReq:Double,
                              adReffReq:Double,
                              adReq:Double,
                              adRTBReqe:Double,
                              adRTBSuccReq:Double,
                              adCost:Double,
                              adPay:Double,
                              adShow:Double,
                              adClick:Double
                             )
/**
  * 网络类型样例类
  */
case class RtpDevicenameAnalysis(devicename:String,
                              adRawReq:Double,
                              adReffReq:Double,
                              adReq:Double,
                              adRTBReqe:Double,
                              adRTBSuccReq:Double,
                              adCost:Double,
                              adPay:Double,
                              adShow:Double,
                              adClick:Double
                             )

/**
  *  区域的统计
  */
case class RptAreaAnalysis(pname:String,
                           cname:String,
                           adRawReq:Double,
                           adReffReq:Double,
                           adReq:Double,
                           adRTBReqe:Double,
                           adRTBSuccReq:Double,
                           adCost:Double,
                           adPay:Double,
                           adShow:Double,
                           adClick:Double
                          )
/**
  *  媒体统计样例类
  */
case class RptappAnalysis(appname:String,
                           adRawReq:Double,
                           adReffReq:Double,
                           adReq:Double,
                           adRTBReqe:Double,
                           adRTBSuccReq:Double,
                           adCost:Double,
                           adPay:Double,
                           adShow:Double,
                           adClick:Double
                          )

