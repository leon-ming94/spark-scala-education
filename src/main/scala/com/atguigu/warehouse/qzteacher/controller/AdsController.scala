package com.atguigu.warehouse.qzteacher.controller

import com.atguigu.warehouse.qzteacher.service.AdsQzService
import com.atguigu.warehouse.utils.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("ads_qz_controller")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    //        HiveUtil.openCompression(sparkSession) //开启压缩
    //    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    val dt = "20190722"
    AdsQzService.getTarget(sparkSession, dt)
    AdsQzService.getTargetApi(sparkSession, dt)
  }
}
