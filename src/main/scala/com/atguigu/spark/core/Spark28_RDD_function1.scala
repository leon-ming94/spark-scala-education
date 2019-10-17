package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object Spark28_RDD_function1 {
    def main(args: Array[String]): Unit = {
        // TODO 1. 创建Spark配置对象
        val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.registerKryoClasses(Array(classOf[Searcher]))

        // TODO 2. 创建Spark环境连接对象
        val sc = new SparkContext(sparkConf)

        // TODO 3. 构建RDD
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

        val searcher = new Searcher("hello")

        //        val result: RDD[String] = searcher.getMatchedRDD1(rdd)
        val result: RDD[String] = searcher.getMatchedRDD2(rdd)

        result.foreach(println)

        // TODO 9. 释放连接
        sc.stop()
    }
}

class Searcher(val query: String) extends Serializable {
    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String) = {
        s.contains(query)
    }

    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) = {
        rdd.filter(this.isMatch) //
    }

    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD2(rdd: RDD[String]) = {
//        val q = query
        rdd.filter(_.contains(query))
    }
}
