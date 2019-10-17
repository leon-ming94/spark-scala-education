package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object TestTransform_combineByKey {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        //创建一个 pairRDD，根据 key 计算每种 key 的value的平均值
        val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
        //转换结构(s,int) >> (s,(sum,count))
        val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
            x => (x, 1),
            (t: (Int, Int), num) => (t._1 + num, t._2 + 1),
            (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
        )
        val resultRDD: RDD[(String, Int)] = combineRDD.map {
            case (word, (sum, count)) => (word, sum / count)
        }
        println(resultRDD.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }
}
