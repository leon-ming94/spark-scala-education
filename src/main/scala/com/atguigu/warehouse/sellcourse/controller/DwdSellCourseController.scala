package com.atguigu.warehouse.sellcourse.controller

import com.atguigu.warehouse.sellcourse.service.DwdSellCourseService
import com.atguigu.warehouse.utils.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdSellCourseController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_sellcourse_import")//.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    DwdSellCourseService.importSaleCourseLog(ssc, sparkSession)
    DwdSellCourseService.importCoursePay(ssc, sparkSession)
    DwdSellCourseService.importCourseShoppingCart(ssc, sparkSession)
  }
}
