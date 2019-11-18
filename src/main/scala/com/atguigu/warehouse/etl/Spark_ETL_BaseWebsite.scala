package com.atguigu.warehouse.etl

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.bean.{BaseWebsite, MemberRegtype}
import com.atguigu.warehouse.utils.{JSONUtils, TextfileDatasetUtil}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author shkstart
  */
object Spark_ETL_BaseWebsite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .appName(this.getClass.getName)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()

        val dataSet: Dataset[String] = TextfileDatasetUtil
                .getTextfileDataSet(spark,"/user/atguigu/ods/baswewebsite.log")

        import spark.implicits._
        val filterDataSet: Dataset[String] = dataSet.filter(line => JSONUtils.verifyIsJSON(line, classOf[BaseWebsite]))

        val resutlDataset: Dataset[BaseWebsite] = filterDataSet.map(line => {
            val value: BaseWebsite = JSON.parseObject(line, classOf[BaseWebsite])
            value
        })

        resutlDataset
                .write
                .format("hive")
                .mode("overwrite")
                .insertInto("dwd.dwd_base_website")
    }
}
