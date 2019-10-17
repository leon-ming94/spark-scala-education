package com.atguigu.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @author shkstart
  */
object SparkSql_readOrWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSql_readOrWrite").enableHiveSupport().getOrCreate()
        import spark.implicits._

        spark.sql("show databases").show()
    }
}
