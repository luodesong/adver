package com.luodesong.adver.util

import com.luodesong.adver.udf.QFUdf
import org.apache.spark.sql.SparkSession

object RegisterUtil {
    /**
      * UDF 注册
      */
    def registerFun(spark: SparkSession): Unit = {
        // 处理年龄段
        spark.udf.register("getAgeRange", QFUdf.getAgeRange _)
    }
}
