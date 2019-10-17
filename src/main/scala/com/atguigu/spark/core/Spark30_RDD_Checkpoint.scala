package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object Spark30_RDD_Checkpoint {
    def main(args: Array[String]): Unit = {
        // TODO 1. 创建Spark配置对象
        val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")
        import org.apache.spark.HashPartitioner
        // TODO 2. 创建Spark环境连接对象
        val sc = new SparkContext(sparkConf)

        // 设定检查点的保存路径
        // 生产环境中，检测点保存路径一般保存到HDFS上
        sc.setCheckpointDir("cp")

        // TODO 3. 构建RDD
        val rdd1 = sc.parallelize(Array("abc"))
        val rdd2: RDD[String] = rdd1.map(_ + " : " + System.currentTimeMillis())
        rdd2.cache()
        rdd2.checkpoint()
        println(rdd2.toDebugString)
        println("***********************")
        // 检查点执行的时候，会在正常逻辑执行完成后，在从头执行一次。
        rdd2.collect().foreach(println)
        rdd2.collect().foreach(println)
        rdd2.collect().foreach(println)

        println(rdd2.toDebugString)

        // TODO 9. 释放连接
        sc.stop()
    }
}
