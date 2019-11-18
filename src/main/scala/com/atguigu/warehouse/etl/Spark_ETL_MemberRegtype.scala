package com.atguigu.warehouse.etl

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.bean.MemberRegtype
import com.atguigu.warehouse.utils.{JSONUtils, TextfileDatasetUtil}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author shkstart
  */
object Spark_ETL_MemberRegtype {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .appName(this.getClass.getName)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()

        val dataSet: Dataset[String] = TextfileDatasetUtil
                .getTextfileDataSet(spark,"/user/atguigu/ods/memberRegtype.log")

        import spark.implicits._

        val filterDataSet: Dataset[String] = dataSet.filter(line => JSONUtils.verifyIsJSON(line, classOf[MemberRegtype]))

        val resutlDataset: Dataset[MemberRegtype] = filterDataSet.map(line => {
            val value: MemberRegtype = JSON.parseObject(line, classOf[MemberRegtype])
            value
        })

//        resutlDataset.show()
        resutlDataset
                .write
                .format("hive")
                .mode("overwrite")
                .insertInto("dwd.dwd_member_regtype")
    }
}
