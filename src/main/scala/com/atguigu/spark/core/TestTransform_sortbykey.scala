package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object TestTransform_sortbykey {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        //        val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))
        //        val resultRDD: RDD[(Int, String)] = rdd.sortBy ({
        //            case (num, word) => word
        //        },false)
        //        println(rdd.sortByKey(false).collect().mkString(","))
        //        println(resultRDD.collect().mkString(","))

//        val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))
//        println(rdd.mapValues(s => s + 1).collect().mkString(","))

        val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "b"), (2, "c")))
        val rdd2: RDD[(Int, String)] = sc.makeRDD(Array((1, "aa"), (3, "bb"), (2, "cc")))
        println(rdd1.join(rdd2).collect().mkString(","))
        println(rdd1.cogroup(rdd2).collect().mkString(","))


        //TODO 3 释放资源
    }
}
