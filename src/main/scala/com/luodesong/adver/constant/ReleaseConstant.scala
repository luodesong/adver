package com.luodesong.adver.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * 常量工具类
  */
object ReleaseConstant {

    // 缓存的模式
    val DEF_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK
    //写入的模式
    val DEF_SAVEMODE: SaveMode = SaveMode.Overwrite

    //分区的字段标志
    val DEF_PARTITION: String = "bdp_day"

    //分区数
    val DEF_SOURCE_PARTITIONS = 4

    // 维度列
    val COL_RELEASE_SESSION_STATUS: String = "release_status"

    // ods==============================
    val ODS_RELEASE_SESSION = "ods_adver.ods_01_release_session"

    // dw===============================
    val DW_RELEASE_CUSTOMER = "dw_adver.dw_release_customer"

    val DW_RELEASE_EXPOSURE = "dw_adver.dw_release_exposure"

    val DW_RELEASE_REGISTER_USERS = "dw_adver.dw_release_register_users"

    val DW_RELEASE_CLICK= "dw_adver.dw_release_click"

    //dm=============================
    //聚合字段
    val COL_RELEASE_DEVICE_NUM = "device_num"
    val COL_RELEASE_USER_COUNT = "user_count"
    val COL_RELEASE_TOTAL_COUNT = "total_count"

    //结果表
    val DM_RELEASE_CUSTOMER_SOURCES ="dm_adver.dm_customer_sources"
    val DM_RELEASE_CUSTOMER_CUBE = "dm_adver.dm_customer_cube"

}
