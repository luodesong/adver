package com.luodesong.adver.util

import com.luodesong.adver.udf.QFUdf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 工具类
  */
object SparkHelperUtil {

    // 处理日志
    val logger: Logger = LoggerFactory.getLogger(SparkHelperUtil.getClass)

    /**
      * 创建 SparkSession
      *
      * @param sconf
      * @return
      */
    def createSpark(sconf: SparkConf): SparkSession = {
        val spark = SparkSession.builder()
                .config(sconf)
                .enableHiveSupport()
                .getOrCreate()

        //注册自定义函数
        RegisterUtil.registerFun(spark)
        spark
    }
}
