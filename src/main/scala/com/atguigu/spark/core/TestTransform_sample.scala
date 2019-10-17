package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_sample {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

//        不放回抽样
        val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
        val rdd2: RDD[Int] = rdd1.sample(false,0.5)
        val rdd3: RDD[Int] = rdd1.sample(false,0.5,2)
        val rdd4: RDD[Int] = rdd1.sample(false,0.5,2)
        val rdd5: RDD[Int] = rdd1.sample(true,2,2)
        val rdd6: RDD[Int] = rdd1.sample(true,2,2)
//        println(rdd2.collect().mkString(","))
//        println(rdd3.collect().mkString(","))
//        println(rdd4.collect().mkString(","))
        println(rdd5.collect().mkString(","))
        println(rdd6.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }

}
