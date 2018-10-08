package com.saintsup.Tags

/**
  * Author by Kaka
  * Created on 2018/10/2 20:38
  */
trait Tag4Others {

  /**
    * 实现不同维度标签
    * @param args 传递参数 可以传递多个
    * @return 返回一个List[(String,Int)]
    */
  def getLabel4User(args:Any*):List[(String,Int)]
}
