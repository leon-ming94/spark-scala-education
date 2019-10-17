package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object Spark_agent_Demo {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        //TODO 3 读取数据 切分数据 (1516609143867 6 7 64 16) 保留( 省份,广告,1)
        //TODO 4 以省份+广告为key,统计广告点击次数 转换结构成((省份,广告),1)
        val rdd: RDD[String] = sc.textFile("input/agent.log")
        val advToOneRDD: RDD[((String, String), Int)] = rdd.map(s => {
            val datas: Array[String] = s.split(" ")
            ((datas(1), datas(4)), 1)
        })

        //TODO 5 统计广告点击次数 reduceByKey(_+_)
        val advToCountRDD: RDD[((String, String), Int)] = advToOneRDD.reduceByKey(_ + _)

        //TODO 6 转换数据结构(省份,(广告,次数)) 以省份为key 以广告点击次数作排序(倒序)
        val advMapRDD: RDD[(String, (String, Int))] = advToCountRDD.map {
            case ((pro, adv), count) => (pro, (adv, count))
        }

        //TODO 7 取前3条数据
        val resultRDD: RDD[(String, List[(String, Int)])] = advMapRDD.groupByKey().map {
            case (pro, t) => (pro, t.toList.sortWith((t1, t2) => t1._2 > t2._2).take(3))
        }
        println(resultRDD.collect().mkString(","))

        //TODO 8 释放资源
        sc.stop()
    }
}
