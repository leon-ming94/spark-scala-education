package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_map {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        //创建一个包含1-10的的 RDD，然后将每个元素*2形成新的 RDD
        val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
        val rdd2: RDD[Int] = rdd1.map(_ * 2)
//        println(rdd2.collect().mkString(","))

        // mapPartitions(func)
        val rdd3: RDD[Int] = rdd1.mapPartitions(_.map(_*2))
//        println(rdd3.collect().mkString(","))

//        mapPartitionsWithIndex(func)
        //过滤掉分区1的数据 只留下分区2的 对分区2中的数据求和
        val rdd4: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index,list) => list.map((index,_)))
        val rdd5: RDD[(Int, Int)] = rdd4.filter(_._1 != 1).reduceByKey(_+_)
        val rdd6: Int = rdd4.filter(_._1 == 0).map(_._2).sum().toInt

        println(rdd5.collect().mkString(","))
        println(rdd6)
        //TODO 3 释放资源
        sc.stop()

    }

}
