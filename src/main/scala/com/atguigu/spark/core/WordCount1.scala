package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val wordRDD: RDD[String] = sc.textFile("input/*.txt")

        val wordFlatMap: RDD[String] = wordRDD.flatMap(_.split(" "))
        val wordGroup: RDD[(String, Iterable[String])] = wordFlatMap.groupBy(word => word)
        val wordCount: RDD[(String, Int)] = wordGroup.map {
            case (word, iter) => {
                (word, iter.size)
            }
        }

        println(wordCount.collect().mkString(","))


        //关闭资源
        sc.stop()
    }
}
