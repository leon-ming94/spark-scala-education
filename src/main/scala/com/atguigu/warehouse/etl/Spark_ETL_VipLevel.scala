package com.atguigu.warehouse.etl

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.bean.{MemberRegtype, VipLevel}
import com.atguigu.warehouse.utils.{JSONUtils, TextfileDatasetUtil}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author shkstart
  */
object Spark_ETL_VipLevel {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .appName(this.getClass.getName)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()

        val dataSet: Dataset[String] = TextfileDatasetUtil
                .getTextfileDataSet(spark,"/user/atguigu/ods/pcenterMemViplevel.log")

        import spark.implicits._

        val filterDataSet: Dataset[String] = dataSet.filter(line => JSONUtils.verifyIsJSON(line, classOf[VipLevel]))

        val resutlDataset: Dataset[VipLevel] = filterDataSet.map(line => {
            val value: VipLevel = JSON.parseObject(line, classOf[VipLevel])
            value
        })

        //        resutlDataset.show()
        resutlDataset
                .write
                .format("hive")
                .mode("overwrite")
                .insertInto("dwd.dwd_vip_level")
    }
}
