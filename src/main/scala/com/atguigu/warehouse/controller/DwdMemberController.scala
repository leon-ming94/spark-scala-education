package com.atguigu.warehouse.controller

import com.atguigu.warehouse.service.EtlDataService
import com.atguigu.warehouse.utils.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author shkstart
  */
object DwdMemberController {
    def main(args: Array[String]): Unit = {
        //TODO 上下文环境
        val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName)//.master("local[*]")
                .enableHiveSupport().getOrCreate()
        val sc: SparkContext = spark.sparkContext
        import spark.implicits._
        sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
        HiveUtil.openDynamicPartition(spark)
        HiveUtil.openCompression(spark)

        //TODO service 导入数据
        EtlDataService.etlBaseAdLog(sc, spark) //导入基础广告表数据
        EtlDataService.etlBaseWebSiteLog(sc, spark) //导入基础网站表数据
        EtlDataService.etlMemberLog(sc, spark) //清洗用户数据
        EtlDataService.etlMemberRegtypeLog(sc, spark) //清洗用户注册数据
        EtlDataService.etlMemPayMoneyLog(sc, spark) //导入用户支付情况记录
        EtlDataService.etlMemVipLevelLog(sc, spark) //导入vip基础数据

    }
}
