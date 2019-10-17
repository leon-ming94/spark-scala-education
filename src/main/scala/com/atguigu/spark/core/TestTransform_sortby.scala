package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_sortby {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(Array(1,3,4,10,4,6,9,20,30,16))
        val rdd2: RDD[Int] = rdd1.sortBy(x => x,false)
        println(rdd2.collect().mkString(","))


        //TODO 3 释放资源
        sc.stop()
    }
}
