package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object Spark29_RDD_Cache {
    def main(args: Array[String]): Unit = {
        // TODO 1. 创建Spark配置对象
        val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

        // TODO 2. 创建Spark环境连接对象
        val sc = new SparkContext(sparkConf)

        // TODO 3. 构建RDD
        val rdd1 = sc.parallelize(Array("ab", "bc"))
        val rdd2 = rdd1.flatMap(x => {
            println("flatMap...")
            x.split("")
        })
        val rdd3: RDD[(String, Int)] = rdd2.map(x => {
            (x, 1)
        })
        // 将RDD的结果持久化到内存中，这样会在依赖关系中增加和缓存相关的依赖
        // 多次执行相关操作时，会从缓存中获取数据，而不是从头执行。提高性能
        // cache操作是不会中断血缘关系，因为数据可能丢失，一旦丢失，需要从头再执行。
//        rdd3.cache()
        // cache只能将数据存储到内存中。persist默认存储数据到内存中，但是可以进行更改
        rdd3.persist()

        rdd3.collect.foreach(println)
        println("-----------")
        println(rdd3.toDebugString)
        rdd3.collect.foreach(println)


        // TODO 9. 释放连接
        sc.stop()
    }
}
