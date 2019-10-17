package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_flatmap {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        //创建一个元素为 1-5 的RDD，运用 flatMap创建一个新的 RDD，
        // 新的 RDD 为原 RDD 每个元素的 平方和三次方 来组成 1,1,4,8,9,27..
        val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5))
        val rdd2: RDD[Int] = rdd1.flatMap(x => List(x*x,x*x*x))
        println(rdd2.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }
}
