package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @author shkstart
  */
object TestTransform_partitionBy {
    def main(args: Array[String]): Unit = {
        //TODO 1 创建配置文件
        val conf: SparkConf = new SparkConf().setAppName("TestRDD2").setMaster("local[4]")

        //TODO 2 创建上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")),4)
        val rddParition: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(3))
        val resultRDD: RDD[(Int, (Int, String))] = rddParition.mapPartitionsWithIndex((index,t) => t.map(t => (index,t)))
        println(resultRDD.collect().mkString(","))

        //TODO 3 释放资源
        sc.stop()
    }
}

class MyPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = 2
}
