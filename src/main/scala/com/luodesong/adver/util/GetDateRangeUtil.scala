package com.luodesong.adver.util

import com.luodesong.adver.util.SparkHelper.logger

import scala.collection.mutable.ArrayBuffer

object GetDateRangeUtil {
    /**
      * 参数校验
      */
    def rangeDates(begin: String, end: String): Seq[String] = {
        val bdp_days = new ArrayBuffer[String]()
        try {
            val bdp_date_begin = DateUtil.dateFormat4String(begin, "yyyyMMdd")
            val bdp_date_end = DateUtil.dateFormat4String(end, "yyyyMMdd")

            if (begin.equals(end)) {
                bdp_days.+=(bdp_date_begin)
            } else {
                var cday = bdp_date_begin
                while (cday != bdp_date_end) {
                    bdp_days.+=(cday)
                    val pday = DateUtil.dateFormat4StringDiff(cday, 1)
                    cday = pday
                }
            }
        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        }
        bdp_days
    }
}
