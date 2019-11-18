package com.atguigu.warehouse.controller

import com.atguigu.warehouse.service.AdsMemberService
import com.atguigu.warehouse.utils.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author shkstart
  */
object AdsMemberController {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("ads_member_controller")//.setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val ssc = sparkSession.sparkContext
        ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
        HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
        AdsMemberService.queryDetailSql(sparkSession,"20190722")
    }
}
