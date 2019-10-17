package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object TestTransform_groupByKey {
    def main(args: Array[String]): Unit = {

        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello", "world", "atguigu", "hello", "are", "go","atguigu","atguigu","go"))
        val mapRDD: RDD[(String, Int)] = rdd.map((_,1))
        val groupbyRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        val resultRDD: RDD[(String, Int)] = groupbyRDD.mapValues(_.sum)
        println(groupbyRDD.collect().mkString(","))
        println(resultRDD.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }
}
