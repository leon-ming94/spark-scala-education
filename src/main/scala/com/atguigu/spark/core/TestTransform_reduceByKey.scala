package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object TestTransform_reduceByKey {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("female",1),("male",5),("female",5),("male",2)))

        val resultRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)
        val resultRDD2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)
        println(resultRDD.collect().mkString(","))
        println(resultRDD2.collect().mkString(","))


        //TODO 3 释放资源
        sc.stop()
    }
}
