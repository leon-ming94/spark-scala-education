package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestRDD01 {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)


        //创建RDD 方式一 从集合中创建
//        val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4))
//
//        val rdd2: RDD[Int] = sc.makeRDD(List(1,2,3,4),3)
//
//        rdd2.saveAsTextFile("output")

        //方拾二 •	从外部存储创建 RDD
        val rdd1: RDD[String] = sc.textFile("input/1.txt")
        rdd1.saveAsTextFile("output")

        //TODO 3 释放资源
        sc.stop()
    }

}
