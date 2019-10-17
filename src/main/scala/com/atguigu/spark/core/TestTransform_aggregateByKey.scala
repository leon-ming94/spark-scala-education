package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object TestTransform_aggregateByKey {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
        //创建一个 pairRDD，取出每个分区相同key对应值的最大值，然后相加
        val resultRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)((x,y) => math.max(x,y),_+_)
        println(resultRDD.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }
}
