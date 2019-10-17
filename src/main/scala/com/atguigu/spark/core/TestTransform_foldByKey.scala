package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object TestTransform_foldByKey {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
        val resutlRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
        println(resutlRDD.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }
}
