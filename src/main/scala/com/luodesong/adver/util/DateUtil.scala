package com.luodesong.adver.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
  * 数据处理工具类
  */
object DateUtil {

    def dateFormat4String(date: String, formater: String = "yyyyMMdd"): String = {
        if (null == date) {
            return null
        }

        val formatter = DateTimeFormatter.ofPattern(formater)
        val datetime = LocalDate.parse(date, formatter)

        datetime.format(DateTimeFormatter.ofPattern(formater))
    }


    def dateFormat4StringDiff(date: String, diff: Long, formater: String = "yyyyMMdd"): String = {
        if (null == date) {
            return null
        }

        val formatter = DateTimeFormatter.ofPattern(formater)
        val datetime = LocalDate.parse(date, formatter)

        val resultDatetime = datetime.plusDays(diff)
        resultDatetime.format(DateTimeFormatter.ofPattern(formater))
    }
}
