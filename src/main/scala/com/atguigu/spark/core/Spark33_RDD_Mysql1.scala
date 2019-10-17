package com.atguigu.spark.core

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object Spark33_RDD_Mysql1 {
    def main(args: Array[String]): Unit = {
        // TODO 1. 创建Spark配置对象
        val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

        // TODO 2. 创建Spark环境连接对象
        val sc = new SparkContext(sparkConf)

        // TODO 3. 向Mysql中写入数据
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop112:3306/test"
        val userName = "root"
        val passWd = "123456"

        val dataRDD: RDD[(Int, String, Int)] = sc.makeRDD(List( (1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40) ))

        /*
        // 下面的代码效率不高，因为频繁创建连接对象.
        dataRDD.foreach{
          case ( id, name, age ) => {
            Class.forName(driver)
            val conn = DriverManager.getConnection(url, userName, passWd)
            val statement: PreparedStatement = conn.prepareStatement("insert into user (id, name, age) values (?, ?, ?)")
            statement.setInt(1, id)
            statement.setString(2, name)
            statement.setInt(3, age)
            statement.executeUpdate()
            statement.close()
            conn.close()
          }
        }
         */

        /*
        Class.forName(driver)
        val conn = DriverManager.getConnection(url, userName, passWd)
        val statement: PreparedStatement = conn.prepareStatement("insert into user (id, name, age) values (?, ?, ?)")

        dataRDD.foreach{
          case ( id, name, age ) => {
            statement.setInt(1, id)
            statement.setString(2, name)
            statement.setInt(3, age)
            statement.executeUpdate()
          }
        }
        statement.close()
        conn.close()
         */

        dataRDD.foreachPartition(datas=>{
            Class.forName(driver)
            val conn = DriverManager.getConnection(url, userName, passWd)
            val statement: PreparedStatement = conn.prepareStatement("insert into user (id, name, age) values (?, ?, ?)")

            datas.foreach{
                case ( id, name, age ) => {
                    statement.setInt(1, id)
                    statement.setString(2, name)
                    statement.setInt(3, age)
                    statement.executeUpdate()
                }
            }

            statement.close()
            conn.close()
        })


        // TODO 9. 释放连接
        sc.stop()
    }
}
