package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_distinct {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

//        对 RDD 中元素执行去重操作. 参数表示任务的数量.默认值和分区数保持一致.
        val rdd1: RDD[Int] = sc.makeRDD(Array(10,10,2,5,3,5,3,6,9,1))
        val rdd2: RDD[Int] = rdd1.distinct(10)
        println(rdd2.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }

}
