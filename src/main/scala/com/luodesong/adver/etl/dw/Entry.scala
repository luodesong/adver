package com.luodesong.adver.etl.dw

object Entry {
    def main(args: Array[String]): Unit = {
        // 如果没有Windows下的hadoop环境变量的话，需要内部执行，自己加载，如果有了，那就算了
        //System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
        val appName :String = "dw_release_customer_job"
        val bdp_day_begin:String ="20190613"
        val bdp_day_end:String ="20190622"
        // 执行Job
        //TheHandler.theCustomerHandleJob(appName,bdp_day_begin,bdp_day_end)
        //exposure执行job
        //TheHandler.theExposureHandleJob(appName,bdp_day_begin,bdp_day_end)
        //注册主题的job
        //TheHandler.theRegisteHandleJob(appName,bdp_day_begin,bdp_day_end)
        //点击主题的job
        TheHandler.theClickHandleJob(appName,bdp_day_begin,bdp_day_end)

    }

}
