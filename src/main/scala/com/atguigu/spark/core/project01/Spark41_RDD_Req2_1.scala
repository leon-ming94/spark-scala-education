package com.atguigu.spark.core.project01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

/**
  * @author shkstart
  */
object Spark41_RDD_Req2_1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark42_RDD_Req1_1").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        //TODO 获取原始数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
        val mapRDD: RDD[UserVisitAction] = dataRDD.map(line => {
            val data: Array[String] = line.split("_")
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

        //使用累加器
        val accumulator: MyCategoryCountAccumulator2 = new MyCategoryCountAccumulator2
        sc.register(accumulator)

        mapRDD.foreach(action => {
            accumulator.add(action)
        })

        val hashMap: mutable.HashMap[(String, String), Long] = accumulator.value
        val groupBy: Map[String, mutable.HashMap[(String, String), Long]] = hashMap.groupBy(t => t._1._1)
        val mapData: immutable.Iterable[(String, Long, Long, Long)] = groupBy.map {
            case (id, map) => {
                (id, map.getOrElse((id, "click"), 0L), map.getOrElse((id, "order"), 0L), map.getOrElse((id, "pay"), 0L))
            }
        }
        val result: List[(String, Long, Long, Long)] = mapData.toList.sortWith((t1, t2) => {
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
        }).take(10)

        //*************************需求2***********************
        //Top10热门品类中每个品类的 Top10 活跃 Session 统计
        val catagoryIds: List[String] = result.map(t => t._1)
        //过滤掉不符合要求的数据 只剩click
        val filterRDD: RDD[UserVisitAction] = mapRDD.filter(action => {
            if (action.click_category_id.toString != -1) {
                catagoryIds.contains(action.click_category_id.toString)
            } else {
                false
            }
        })

        //转换数据结构 -> 以(id,sessionId)为key进行聚合
        val reduceByKeyRDD: RDD[((Long, String), Int)] = filterRDD.map(action => ((action.click_category_id, action.session_id), 1)).reduceByKey(_ + _)

        //转换数据结构 -> 以id为key进行分组 组内排序取前10条
//        val groupByKeyRDD: RDD[(Long, Iterable[(String, Int)])] = reduceByKeyRDD.map {
//            case ((id, sessionId), num) => {
//                (id, (sessionId, num))
//            }
//        }.groupByKey()
//        val resultRDD: RDD[(Long, List[(String, Int)])] = groupByKeyRDD.map(data => {
//            val list: List[(String, Int)] = data._2.toList.sortWith((t1, t2) => t1._2 > t2._2).take(10)
//            (data._1, list)
//        })
//        resultRDD.collect().foreach(println)

        val mapReduceRDD: RDD[(Long, (String, Int))] = reduceByKeyRDD.map {
            case ((id, sessionId), num) => {
                (id, (sessionId, num))
            }
        }
        val listBuffer: ListBuffer[Array[(Long, (String, Int))]] = new mutable.ListBuffer[Array[(Long, (String, Int))]]()
        catagoryIds.foreach(id =>{
            val filter: RDD[(Long, (String, Int))] = mapReduceRDD.filter(t => t._1.toString == id)
            val tuples: Array[(Long, (String, Int))] = filter.sortBy(t=>t._2._2,false).take(10)
            listBuffer.append(tuples)
        })

        listBuffer.foreach(data => println(data.mkString(",")))
        sc.stop()
    }
}
