package com.luodesong.adver.util

import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}


/**
  * 生成conf
  */
object ConfHelper {
    // 日志处理
    val logger: Logger = LoggerFactory.getLogger(ConfHelper.getClass)

    def createConf(appName: String): SparkConf = {
        var conf: SparkConf = null;
        try {
            // spark 配置参数
            conf = new SparkConf()
            conf.set("hive.exec.dynamic.partition", "true")
                .set("hive.exec.dynamic.partition.mode", "nonstrict")
                .set("spark.sql.shuffle.partitions", "32")
                .set("hive.merge.mapfiles", "true")
                .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
                .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
                .set("spark.sql.crossJoin.enabled", "true")
                //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
                .setAppName(appName)
                .setMaster("local[4]")
        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
                null
            }
        } finally {
            if (conf != null) {
                conf
            }
        }
    }

}
