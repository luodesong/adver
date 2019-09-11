package com.luodesong.adver.etl.dm

import scala.collection.mutable.ArrayBuffer

/**
  * 用于聚合分组的列
  */
object DMReleaseGroupColumeHelper {

    def groupDMReleaseCustomerColumns(): ArrayBuffer[String] = {
        val columns = new ArrayBuffer[String]()
        columns.append("sources")
        columns.append("channels")
        columns.append("device_type")
        columns.append("bdp_day")
        columns
    }

    def cubDMReleaseCustomerColumns(): ArrayBuffer[String] = {
        val columns = new ArrayBuffer[String]()
        columns.append("sources")
        columns.append("channels")
        columns.append("device_type")
        columns.append("age_range")
        columns.append("gender")
        columns.append("area_code")
        columns
    }

}
