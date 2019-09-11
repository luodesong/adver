package com.luodesong.adver.etl.dw

import com.luodesong.adver.util.{ConfHelperUtil, GetDateRangeUtil, SparkHelperUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class DWTheHandler {

}

object DWTheHandler {

    // 日志处理
    val logger: Logger = LoggerFactory.getLogger(DWTheHandler.getClass)

    /**
      * 点击主题
      *
      * @param appName
      * @param bdp_day_begin
      * @param bdp_day_end
      */
    def theClickHandleJob(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
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
                DWReleaseHandleJob.clickHandleReleaseJob(spark, bdp_date)
            }
        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        } finally {
            if (spark != null) {
                spark.stop()
            }
        }
    }

    /**
      * 注册主题
      *
      * @param appName
      * @param bdp_day_begin
      * @param bdp_day_end
      */
    def theRegisteHandleJob(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
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
                DWReleaseHandleJob.registeHandleReleaseJob(spark, bdp_date)
            }
        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        } finally {
            if (spark != null) {
                spark.stop()
            }
        }
    }

    /**
      * 曝光主题
      *
      * @param appName
      * @param bdp_day_begin
      * @param bdp_day_end
      */
    def theExposureHandleJob(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
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
                DWReleaseHandleJob.exposureHandleReleaseJob(spark, bdp_date)
            }
        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        } finally {
            if (spark != null) {
                spark.stop()
            }
        }
    }

    /**
      * 投放目标客户
      *
      * @param appName
      * @param bdp_day_begin
      * @param bdp_day_end
      */
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
                DWReleaseHandleJob.customerHandleReleaseJob(spark, bdp_date)
            }
        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        } finally {
            if (spark != null) {
                spark.stop()
            }
        }
    }

}
