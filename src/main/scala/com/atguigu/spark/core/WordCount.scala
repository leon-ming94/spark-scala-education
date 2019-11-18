package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart
  */
object WordCount {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val wordRDD: RDD[String] = sc.textFile("input")
        //扁平化
        val wordString: RDD[String] = wordRDD.flatMap(_.split(" "))
        //String --> String , int
        val wordToOneRDD: RDD[(String, Int)] = wordString.map((_,1))
        //reduce
        val wordToCountRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_+_)

        val value: RDD[(String, Int)] = wordToCountRDD.repartition(2)
        //采集
        val tuples: Array[(String, Int)] = value.collect()
//        val tuples: Array[(String, Int)] = wordToCountRDD.collect()
        tuples.foreach(println)
        //关闭资源
        sc.stop()

    }
}
