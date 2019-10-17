package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object Spark31_RDD_JSON {
    def main(args: Array[String]): Unit = {
        // TODO 1. 创建Spark配置对象
        val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

        // TODO 2. 创建Spark环境连接对象
        val sc = new SparkContext(sparkConf)
        import scala.util.parsing.json.JSON
        val stringRDD: RDD[String] = sc.textFile("input/user.json")

        // Spark读取JSON文件，是按照每一行解析的，所以要求，每一行的数据为JSON字符串。
        val jsonRDD = stringRDD.map(JSON.parseFull)

        jsonRDD.collect().foreach(println)

        // TODO 9. 释放连接
        sc.stop()
    }
}
