package com.atguigu.warehouse.etl

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.bean.PcentermemPaymoney
import com.atguigu.warehouse.utils.{JSONUtils, TextfileDatasetUtil}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author shkstart
  */
object Spark_ETL_PcentermemPaymoney {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .appName(this.getClass.getName)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()

        val dataSet: Dataset[String] = TextfileDatasetUtil
                .getTextfileDataSet(spark,"/user/atguigu/ods/pcentermempaymoney.log")

        import spark.implicits._

        val filterDataSet: Dataset[String] = dataSet.filter(line => JSONUtils.verifyIsJSON(line, classOf[PcentermemPaymoney]))

        val resutlDataset: Dataset[PcentermemPaymoney] = filterDataSet.map(line => {
            val value: PcentermemPaymoney = JSON.parseObject(line, classOf[PcentermemPaymoney])
            value
        })

        //        resutlDataset.show()
        resutlDataset
                .write
                .format("hive")
                .mode("overwrite")
                .insertInto("dwd.dwd_pcentermempaymoney")
    }
}
