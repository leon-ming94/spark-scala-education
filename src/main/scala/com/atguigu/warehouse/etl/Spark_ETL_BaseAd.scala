package com.atguigu.warehouse.etl

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.bean.{BaseAd, MemberRegtype}
import com.atguigu.warehouse.utils.{JSONUtils, TextfileDatasetUtil}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author shkstart
  */
object Spark_ETL_BaseAd {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .appName(this.getClass.getName)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()

        val dataSet: Dataset[String] = TextfileDatasetUtil
                .getTextfileDataSet(spark,"/user/atguigu/ods/baseadlog.log")

        import spark.implicits._

        val filterDataSet: Dataset[String] = dataSet.filter(line => JSONUtils.verifyIsJSON(line, classOf[BaseAd]))

        val resutlDataset: Dataset[BaseAd] = filterDataSet.map(line => {
            val value: BaseAd = JSON.parseObject(line, classOf[BaseAd])
            value
        })

        resutlDataset
                .write
                .format("hive")
                .mode("overwrite")
                .insertInto("dwd.dwd_base_ad")

    }
}
