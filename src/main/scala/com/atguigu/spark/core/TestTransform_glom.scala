package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_glom {

    def main(args: Array[String]): Unit = {

        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)
        //    创建一个 4 个分区的 RDD，并将每个分区的数据放到一个数组
        val rdd1: RDD[Int] = sc.makeRDD(Array(10,20,30,40,50,60), 4)
        val rdd2: RDD[Array[Int]] = rdd1.glom()
        rdd2.collect().foreach(a => println(a.mkString(",")))

//        TODO 4. 获取每个分区中的最大值后求和
        val res: Int = rdd2.map(_.max).sum().toInt
        println(res)

        //TODO 3 释放资源
        sc.stop()
    }
}
