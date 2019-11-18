package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @author shkstart
  */
object WordCount10 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val wordRDD: RDD[String] = sc.textFile("input/*.txt")

        val wordFlatmap: RDD[(String, Int)] = wordRDD.flatMap(word => {
            val data: mutable.ArrayOps[String] = word.split(" ")
            data.map((_, 1))
        })

        //TODO 第一种 groupby
        val wordGroupby: RDD[(String, Iterable[(String, Int)])] = wordFlatmap.groupBy(t => t._1)
        val wordCount1: RDD[(String, Int)] = wordGroupby.map {
            case (word, iter) => {
                (word, iter.size)
            }
        }
        //        println(wordCount1.collect().mkString(","))

        //TODO 第二种 groupbyKey
        val wordCount2: RDD[(String, Int)] = wordFlatmap.groupByKey().map {
            case (word, iter) => {
                (word, iter.size)
            }
        }
        //        println(wordCount2.collect().mkString(","))

        //TODO 第三种 reduceByKey
        val wordCount3: RDD[(String, Int)] = wordFlatmap.reduceByKey(_ + _)
        //        println(wordCount3.collect().mkString(","))

        //TODO 第四种 aggregateByKey
        val wordCount4: RDD[(String, Int)] = wordFlatmap.aggregateByKey(0)(_ + _, _ + _)
        //        println(wordCount4.collect().mkString(","))

        //TODO 第五种 foldByKey
        val wordCount5: RDD[(String, Int)] = wordFlatmap.foldByKey(0)(_ + _)
        //        println(wordCount5.collect().mkString(","))

        //TODO 第六种 combineByKey
        val wordCount6: RDD[(String, Int)] = wordFlatmap.combineByKey(x => x, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)
        //        println(wordCount6.collect().mkString(","))

        //TODO 第七种 countByKey
        val wordCount7: collection.Map[String, Long] = wordFlatmap.countByKey()
        //        println(wordCount7.mkString(","))

        //TODO 第八种 countByValue
        val wordCount8: collection.Map[(Int, String), Long] = wordFlatmap.map {
            case (word, num) => {
                (num, word)
            }
        }.countByValue()
        //        println(wordCount8.mkString(","))

        //TODO 第九种 reduce
        val wordMap: RDD[mutable.Map[String, Int]] = wordFlatmap.map {
            case (word, num) => {
                mutable.Map(word -> num)
            }
        }
        val wordCount9: mutable.Map[String, Int] = wordMap.reduce((m1, m2) => {
            m1.foldLeft(m2)((map, kv) => {
                val key: String = kv._1
                val value: Int = kv._2
                map(key) = map.getOrElse(key, 0) + value
                map
            })
        })
        //        println(wordCount9.mkString(","))

        //TODO 第十种 fold
        val wordFold: mutable.Map[String, Int] = wordMap.fold(mutable.Map("first" -> 0))((m1, m2) => {
            m1.foldLeft(m2)((map, kv) => {
                val key: String = kv._1
                val value: Int = kv._2
                map(key) = map.getOrElse(key, 0) + value
                map
            })
        })
        val wordCount10: mutable.Map[String, Int] = wordFold.filter(t => !t._1.equals("first"))
        println(wordCount10.mkString(","))

        sc.stop()
    }

}
