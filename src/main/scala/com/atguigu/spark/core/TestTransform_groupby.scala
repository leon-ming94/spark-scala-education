package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_groupby {

    def main(args: Array[String]): Unit = {

        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

//        创建一个 RDD，按照元素的奇偶性进行分组
        val rdd1: RDD[Int] = sc.makeRDD(Array(1, 3, 4, 20, 4, 5, 8))
        val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(_%2)

        println(rdd2.collect().mkString(","))


        //TODO 3 释放资源
        sc.stop()
    }
}
