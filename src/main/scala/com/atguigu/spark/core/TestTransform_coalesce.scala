package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_coalesce {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 100,5)
        val rdd3: RDD[Array[Int]] = rdd1.glom()
        rdd3.collect().foreach(arr => println(arr.mkString(",")))

        val rdd2: RDD[Int] = rdd1.coalesce(2)
        val rdd4: RDD[Array[Int]] = rdd2.glom()
        rdd4.collect().foreach(arr => println(arr.mkString(",")))

        val rdd5: RDD[Int] = rdd1.coalesce(2,true)
        rdd5.glom().collect().foreach(arr => println(arr.mkString(",")))

        val rdd6: RDD[Int] = rdd1.repartition(2)
        rdd6.glom().collect().foreach(arr => println(arr.mkString(",")))

        //TODO 3 释放资源
        sc.stop()
    }
}
