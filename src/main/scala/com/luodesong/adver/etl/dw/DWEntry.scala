package com.luodesong.adver.etl.dw

object DWEntry {
    def main(args: Array[String]): Unit = {
        // 如果没有Windows下的hadoop环境变量的话，需要内部执行，自己加载，如果有了，那就算了
        //System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
        val bdp_day_begin:String ="20190613"
        val bdp_day_end:String ="20190622"
        // 执行Job
        //DWTheHandler.theCustomerHandleJob("dw_release_customer_job",bdp_day_begin,bdp_day_end)
        //exposure执行job
        //DWTheHandler.theExposureHandleJob("dw_release_exposure_job",bdp_day_begin,bdp_day_end)
        //注册主题的job
        //DWTheHandler.theRegisteHandleJob("dw_release_register_job",bdp_day_begin,bdp_day_end)
        //点击主题的job
        DWTheHandler.theClickHandleJob("dw_release_click_job",bdp_day_begin,bdp_day_end)

    }

}
