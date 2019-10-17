package com.atguigu.spark.core

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object Spark32_RDD_Mysql {
    def main(args: Array[String]): Unit = {
        // TODO 1. 创建Spark配置对象
        val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

        // TODO 2. 创建Spark环境连接对象
        val sc = new SparkContext(sparkConf)

        // TODO 3. 从Mysql中读取数据
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop112:3306/test"
        val userName = "root"
        val passWd = "123456"

        val sql = "select * from user where id >= ? and id <= ?"
//        val sql = "select * from user"

        val jdbc = new JdbcRDD(
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            sql,
            1,
            3,
            3,
            (rs) => {
                println(rs.getInt(1) + ", " + rs.getString(2) + "," + rs.getInt(3))
            }
        )
        jdbc.collect

        // TODO 9. 释放连接
        sc.stop()
    }
}
