package com.luodesong.adver.etl.dw

import com.luodesong.adver.constant.ReleaseConstant
import com.luodesong.adver.enums.ReleaseStatusEnum
import com.luodesong.adver.util.ReadAndWriteUtil
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

class DWReleaseCustomer {
}

/**
  * DW 投放目标客户主题
  */
object DWReleaseCustomer {
    // 日志处理
    val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

    /**
      * 目标客户
      * status = "01"
      */
    def handleReleaseJob(spark: SparkSession, bdp_day: String): Unit = {
        try {
            // 导入隐式转换
            import org.apache.spark.sql.functions._
            // 设置缓存级别
            val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
            val saveMode: SaveMode = ReleaseConstant.DEF_SAVEMODE
            // 获取当天日志字段数据
            val cusomerColumns = DWReleaseColumnsHelper.selectDWReleaseColumns()
            // 当天数据，设置条件，根据条件进行查询，后续调用数据

            val cusomerReleaseCondition: Column = (col(s"${ReleaseConstant.DEF_PARTITION}")) === lit(bdp_day) and
                    col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") ===
                            lit(ReleaseStatusEnum.CUSTOMER.getCode)
            // 填入条件
            val customerReleaseDF: DataFrame = ReadAndWriteUtil
                    .readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, cusomerColumns)
                    // 查询条件
                    .where(cusomerReleaseCondition)
                    // 重分区
                    .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)
            println("查询结束======================结果显示")
            customerReleaseDF.show(10, false)
            // 目标用户
            ReadAndWriteUtil.writeTableData(customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, saveMode)

        } catch {
            // 错误信息处理
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        }
    }


}
