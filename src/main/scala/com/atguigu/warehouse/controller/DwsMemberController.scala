package com.atguigu.warehouse.controller

import com.atguigu.warehouse.service.DwsMemberService
import com.atguigu.warehouse.utils.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author shkstart
  */
object DwsMemberController {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName)//.master("local[*]")
                .enableHiveSupport().getOrCreate()
        val sc: SparkContext = spark.sparkContext

        HiveUtil.openDynamicPartition(spark)
        HiveUtil.openCompression(spark)

        DwsMemberService.importMember(spark,"20190722")
    }
}
