package com.luodesong.adver.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object ReadAndWriteUtil {

    // 处理日志
    val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

    /**
      * 读取数据表
      */
    def readTableData(spark: SparkSession, tableNam: String, colNames: mutable.Seq[String]) = {
        val begin = System.currentTimeMillis()
        // 读取表
        val tableDF = spark.read.table(tableNam)
                .selectExpr(colNames: _*)
        tableDF
    }

    /**
      * 写入数据表
      */
    def writeTableData(sourceDF: DataFrame, table: String, mode: SaveMode): Unit = {
        val begin = System.currentTimeMillis()
        // 写入表数据
        sourceDF.write.mode(mode).insertInto(table)
        println(s"table[${table}] use :${System.currentTimeMillis() - begin}-==========")
    }
}
