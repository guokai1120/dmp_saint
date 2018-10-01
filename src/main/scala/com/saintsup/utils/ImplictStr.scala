package com.saintsup.utils

/**
  * 隐式转换 将自己封装的方法隐式的加强到string类上
  * 在调用的时候必须用 implicit com.saintsup.utils.ImplictStr._导入
  * Author by Kaka
  * Created on 2018/9/28 21:49
  */
class ImplictStr(str:String) {

  /**
    * 转换为int
    * @return
    */
  def toIntx:Int={
    try {
      str.toInt
    } catch {
      case _:Exception => 0
    }
  }

  /**
    * 转换为Double
    * @return
    */
  def toDoublex:Double={
    try {
      str.toDouble
    } catch {
      case _:Exception => 0d
    }
  }
}

/**
  * 在方法前面加上implicit就是隐式转换
  */
object ImplictStr{
  /**
    * 隐式转换的方法
    * @param str
    * @return
    */
  implicit def str2ImplictStr(str:String)={
    new ImplictStr(str)
  }
}
