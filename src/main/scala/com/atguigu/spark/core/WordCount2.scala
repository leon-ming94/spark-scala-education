package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @author shkstart
  */
object WordCount2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val wordRDD: RDD[String] = sc.textFile("input/*.txt")

        val wordFlatMap: RDD[(String, Int)] = wordRDD.flatMap(word => {
            val words: mutable.ArrayOps[String] = word.split(" ")
            words.map((_, 1))
        })

        //        val wordCount: RDD[(String, Int)] = wordFlatMap.reduceByKey(_+_)
        //groupByKey
        val wordCount: RDD[(String, Int)] = wordFlatMap.groupByKey().map {
            case (word, iter) => {
                (word, iter.size)
            }
        }
        //aggregateByKey
        val wordCount1: RDD[(String, Int)] = wordFlatMap.aggregateByKey(0)(_ + _, _ + _)

        //flodByKey
        val wordCount2: RDD[(String, Int)] = wordFlatMap.foldByKey(0)(_ + _)

        //combinByKey
        val wordCount3: RDD[(String, Int)] = wordFlatMap.combineByKey(x => x, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)

        //        println(wordFlatMap.countByKey().mkString(","))
        val wordcount4: collection.Map[(Int, String), Long] = wordFlatMap.map {
            case (word, count) => {
                (count, word)
            }
        }.countByValue()
        //        println(wordcount4.mkString(","))
        //        println(wordCount3.collect().mkString(","))

        //reduce

        val wordFlatmap: RDD[mutable.Map[String, Int]] = wordRDD.flatMap(word => {
            val words: mutable.ArrayOps[String] = word.split(" ")
            words.map(word => mutable.Map(word -> 1))
        })
        println(wordFlatmap.collect().mkString(","))
        val resultCount: mutable.Map[String, Int] = wordFlatmap.reduce((m1, m2) => {
            m1.foldLeft(m2)((map, kv) => {
                val key: String = kv._1
                val value: Int = kv._2
                map(key) = map.getOrElse(key, 0) + value
                map
            })
        })
        //        println(resultCount.mkString(","))

        val resultCount2: mutable.Map[String, Int] = wordFlatmap.fold(mutable.Map("test" -> 0))((m1, m2) => {
            m1.foldLeft(m2)((map, kv) => {
                val key: String = kv._1
                val value: Int = kv._2
                map(key) = map.getOrElse(key, 0) + value
                map
            })
        })
        val resultCount3: mutable.Map[String, Int] = resultCount2.filter(t => !t._1.equals("test"))
        println(resultCount3.mkString(","))
        sc.stop()
    }
}
