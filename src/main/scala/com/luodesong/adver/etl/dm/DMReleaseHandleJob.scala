package com.luodesong.adver.etl.dm

import com.luodesong.adver.constant.ReleaseConstant
import com.luodesong.adver.etl.dw.DWReleaseHandleJob
import com.luodesong.adver.util.ReadAndWriteUtil
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class DMReleaseHandleJob {

}

object DMReleaseHandleJob {
    // 日志处理
    val logger: Logger = LoggerFactory.getLogger(DWReleaseHandleJob.getClass)

    def customerHandleReleaseJob(spark: SparkSession, bdp_day: String) = {
        try {
            // 导入隐式转换
            import org.apache.spark.sql.functions._
            // 设置缓存级别
            val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
            val saveMode: SaveMode = ReleaseConstant.DEF_SAVEMODE
            // 获取当天日志字段数据
            val cusomerColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMReleaseCustomerColumns()
            // 当天数据，设置条件，根据条件进行查询，后续调用数据
            val cusomerReleaseCondition: Column = (col(s"${ReleaseConstant.DEF_PARTITION}")) === lit(bdp_day)
            // 填入条件
            val customerReleaseDF: DataFrame = ReadAndWriteUtil
                    .readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, cusomerColumns)
                    // 查询条件
                    .where(cusomerReleaseCondition)
                    //设置缓存级别
                    .persist(storageLevel)
            println("查询结束======================结果显示")
            //customerReleaseDF.show(10, false)

            /**
              *
              * 渠道用户统计
              */

            val resourceColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMCustomerSourceColumns()
            //组合要进行分组的字段
            val groupColumns: ArrayBuffer[String] = DMReleaseGroupColumeHelper.groupDMReleaseCustomerColumns()
            //将字段名转化为列名
            val columns = new ArrayBuffer[Column]()
            for (t <- groupColumns) {
                columns.append(col(t))
            }
            //columns:_*的作用是将columns压平成一个个的cloumn,然后按着这些列进行分组
            val resourceCustomerDF: DataFrame = customerReleaseDF.groupBy(columns: _*).agg(
                countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
                count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
            ).selectExpr(resourceColumns: _*)
            //resourceCustomerDF.show(30, false)
            //将数据存储出去
            ReadAndWriteUtil.writeTableData(resourceCustomerDF, ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCES, saveMode)

            /**
              *
              * 渠道用户统计cub

            val cubResourceColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.cubDMCustomerSourceColumns()
            //组合cub要进行分组的字段
            val cubColumns: ArrayBuffer[String] = DMReleaseGroupColumeHelper.cubDMReleaseCustomerColumns()
            //将字段名转化为列名
            val columns1 = new ArrayBuffer[Column]()
            for (t <- cubColumns) {
                columns1.append(col(t))
            }
            //columns:_*的作用是将columns压平成一个个的cloumn,然后按着这些列进行分组
            val cubCustomerDF: DataFrame = customerReleaseDF.cube(columns1: _*).agg(
                countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
                count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
            ).withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day)).selectExpr(cubResourceColumns: _*)
            //cubCustomerDF.show(30, false)
            ReadAndWriteUtil.writeTableData(cubCustomerDF, ReleaseConstant.DM_RELEASE_CUSTOMER_CUBE, saveMode)
              */

        } catch {
            // 错误信息处理
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        }
    }
}
