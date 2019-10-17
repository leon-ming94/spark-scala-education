package com.atguigu.spark.core.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}


/**
  * @author shkstart
  */
object Spark42_RDD_Req1_1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark42_RDD_Req1_1").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        //TODO 获取原始数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
        val mapRDD: RDD[UserVisitAction] = dataRDD.map(line => {
            val data: Array[String] = line.split("_")
            //            2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
            UserVisitAction(
                data(0),
                data(1).toLong,
                data(2),
                data(3).toLong,
                data(4),
                data(5),
                data(6).toLong,
                data(7).toLong,
                data(8),
                data(9),
                data(10),
                data(11),
                data(12).toLong
            )
        })
        val accumulator: MyCategoryCountAccumulator = new MyCategoryCountAccumulator
        sc.register(accumulator)

        mapRDD.foreach(action => {
            accumulator.add(action)
        })

        val accumulatorValue: mutable.HashMap[(String, String), Long] = accumulator.value
        val groupData: Map[String, mutable.HashMap[(String, String), Long]] = accumulatorValue.groupBy(t => t._1._1)
        //        println(groupData.mkString(","))
        val tuples: immutable.Iterable[(String, Long, Long, Long)] = groupData.map {
            case (id, map) => {
                (id, map.getOrElse((id, "click"), 0L), map.getOrElse((id, "order"), 0L), map.getOrElse((id, "pay"), 0L))
            }
        }
        val list: List[(String, Long, Long, Long)] = tuples.toList
        val result: List[(String, Long, Long, Long)] = list.sortWith((t1, t2) => {
            if (t1._2 > t2._2) {
                true
            } else if (t1._2 == t2._2) {
                if (t1._3 > t2._3) {
                    true
                } else if (t1._3 == t2._3) {
                    t1._4 > t2._4
                } else {
                    false
                }
            } else {
                false
            }
        })
        result.foreach(println)

        sc.stop()
    }
}

class MyCategoryCountAccumulator extends AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]] {

    private var map: mutable.HashMap[(String, String), Long] = new mutable.HashMap[(String, String), Long]()

    override def isZero: Boolean = {
        map.isEmpty
    }

    override def copy(): AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]] = {
        new MyCategoryCountAccumulator
    }

    override def reset(): Unit = {
        map.clear()
    }

    override def add(v: UserVisitAction): Unit = {
        if (v.click_category_id != -1) {
            val key = (v.click_category_id.toString, "click")
            map(key) = map.getOrElse(key, 0L) + 1L
        } else if (v.order_category_ids != "null") {
            val ids: Array[String] = v.order_category_ids.split(",")
            ids.foreach(id => {
                val key: (String, String) = (id, "order")
                map(key) = map.getOrElse(key, 0L) + 1L
            })
        } else if (v.pay_category_ids != "null") {
            val ids: Array[String] = v.pay_category_ids.split(",")
            ids.foreach(id => {
                val key: (String, String) = (id, "pay")
                map(key) = map.getOrElse(key, 0L) + 1L
            })
        }
    }

    override def merge(other: AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]]): Unit = {
        map = map.foldLeft(other.value)((innerMap, t) => {
            val key: (String, String) = t._1
            innerMap(key) = innerMap.getOrElse(key, 0L) + t._2
            innerMap
        })
    }

    override def value: mutable.HashMap[(String, String), Long] = {
        map
    }
}
