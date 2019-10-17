package com.atguigu.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @author shkstart
  */
object SparkSql_01 {
    def main(args: Array[String]): Unit = {
        //获取sparkSession对象
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSql_01").getOrCreate()
        val df: DataFrame = spark.read.json("input/user.json")
        val rdd: RDD[Row] = df.rdd

        import spark.implicits._

        val rdd1: RDD[(String, Long)] = rdd.map(r => (r.getString(1),r.getLong(0)))
        val df1: DataFrame = rdd1.toDF("name","age")

        val rdd2: RDD[User] = rdd.map(r => User(r.getLong(0),r.getString(1)))
        val df3: DataFrame = rdd2.toDF()
        val ds: Dataset[User] = rdd2.toDS()

        ds.createTempView("user")

        spark.sql("select * from user where age > 20").show()

        spark.stop()
    }
}

case class User(age:Long,username:String){

}
