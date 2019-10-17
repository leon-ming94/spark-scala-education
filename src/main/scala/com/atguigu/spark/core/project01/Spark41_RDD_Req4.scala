package com.atguigu.spark.core.project01

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object Spark41_RDD_Req4 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark42_RDD_Req1_1").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        //TODO 获取原始数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
        val actionRDD: RDD[UserVisitAction] = dataRDD.map(line => {
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
        //分母
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val pageIdsSum: collection.Map[(String, Long), Long] = actionRDD.map(action => ((action.session_id, action.page_id), 1)).countByKey()

        //分子
        val groupSessionIdRDD: RDD[(String, Iterable[(String, Long, String)])] = actionRDD.map(action => (action.session_id, action.page_id, action.action_time)).groupBy(t => t._1)

        val pageIdTimeToTwo: RDD[List[((String, Long), Long)]] = groupSessionIdRDD.map {
            case (sessionId, datas) => {
                val sortByTimeRDD: List[(String, Long, String)] = datas.toList.sortWith((t1, t2) => {
                    t1._3 < t2._3
                })
                val zipRDD: List[((String, Long, String), (String, Long, String))] = sortByTimeRDD.zip(sortByTimeRDD.tail)
                val zipMapRDD: List[((String, Long), Long)] = zipRDD.map {
                    case (t1, t2) => {
                        val time1: Long = format.parse(t1._3).getTime
                        val time2: Long = format.parse(t2._3).getTime
                        ((t1._1, t1._2), time2 - time1)
                    }
                }
                zipMapRDD
            }
        }
        val pageIdsTimeTotal: Array[((String, Long), Long)] = pageIdTimeToTwo.flatMap(data => data).reduceByKey(_+_).collect()

        pageIdsTimeTotal.foreach{
            case ((sessionId,pageId),time) =>{
                val count: Long = pageIdsSum.getOrElse((sessionId,pageId.toLong),1L)

                println((sessionId, pageId) + "=" + (time / count))
            }
        }

        sc.stop()
    }
}
