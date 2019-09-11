package com.luodesong.adver.etl.dm

import scala.collection.mutable.ArrayBuffer

/**
  * DM 获取日志字段
  */
object DMReleaseColumnsHelper {

    /**
      * 目标客户
      */
    def selectDMReleaseCustomerColumns(): ArrayBuffer[String] = {
        val columns = new ArrayBuffer[String]()
        columns.+=("release_session")
        columns.+=("release_status")
        columns.+=("device_num")
        columns.+=("device_type")
        columns.+=("sources")
        columns.+=("channels")
        columns.+=("idcard")
        columns.+=("age")
        // 加载UDF函数
        columns.+=("getAgeRange(age) as age_range")
        columns.+=("gender")
        columns.+=("area_code")
        columns.+=("ct")
        columns.+=("bdp_day")
        columns
    }

    def selectDMCustomerSourceColumns():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
        columns.+=("sources")
        columns.+=("channels")
        columns.+=("device_type")
        columns.+=("user_count")
        columns.+=("total_count")
        columns.+=("bdp_day")
        columns
    }

    def cubDMCustomerSourceColumns():ArrayBuffer[String]={
        val columns = new ArrayBuffer[String]()
        columns.+=("sources")
        columns.+=("channels")
        columns.+=("device_type")
        columns.+=("age_range")
        columns.+=("gender")
        columns.+=("area_code")
        columns.+=("user_count")
        columns.+=("total_count")
        columns.+=("bdp_day")
        columns
    }

}
