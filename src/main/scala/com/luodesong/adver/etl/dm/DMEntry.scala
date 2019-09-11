package com.luodesong.adver.etl.dm

object DMEntry {
    def main(args: Array[String]): Unit = {
        val bdp_day_begin:String ="20190613"
        val bdp_day_end:String ="20190622"
        // 执行Job
        DMTheHandler.theCustomerHandleJob("customer",bdp_day_begin,bdp_day_end)
    }
}
