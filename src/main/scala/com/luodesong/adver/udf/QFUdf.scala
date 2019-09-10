package com.luodesong.adver.udf

import com.luodesong.adver.util.CommonUtil

/**
  * spark UDF
  */
object QFUdf {

  /**
    * 年龄段
    */
  def getAgeRange(age:String):String={
    var tseg = ""
    if(null != age){
      try {
        tseg = CommonUtil.getAgeRange(age)
      }catch {
        case ex:Exception=>{
          println(s"$ex")
        }
      }
    }
    tseg
  }
}
