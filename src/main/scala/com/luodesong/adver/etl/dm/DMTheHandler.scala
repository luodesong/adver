package com.luodesong.adver.etl.dm

import com.luodesong.adver.util.{ConfHelperUtil, GetDateRangeUtil, SparkHelperUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class DMTheHandler {

}

object DMTheHandler {
    // 日志处理
    val logger: Logger = LoggerFactory.getLogger(DMTheHandler.getClass)

    def theCustomerHandleJob(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
        var spark: SparkSession = null
        try {
            // 获取到 spark 配置参数
            val conf: SparkConf = ConfHelperUtil.createConf(appName)
            // 获取到 spark 上下文
            spark = SparkHelperUtil.createSpark(conf)
            // 参数校验
            val timeRanges: Seq[String] = GetDateRangeUtil.rangeDates(bdp_day_begin, bdp_day_end)
            for (bdp_day <- timeRanges.reverse) {
                val bdp_date = bdp_day.toString
                DMReleaseHandleJob.customerHandleReleaseJob(spark, bdp_date)
            }

        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        } finally {
            if (spark != null) {
                spark.close()
            }
        }
    }
}