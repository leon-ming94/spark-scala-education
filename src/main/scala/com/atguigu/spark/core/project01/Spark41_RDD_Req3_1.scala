package com.atguigu.spark.core.project01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author shkstart
  */
object Spark41_RDD_Req3_1 {
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

        //分子/分母
        // 分母:所有页码id点击次数 (pageId,count)
        val filterList: List[Long] = List(1,2,3,4,5,6,7)
        val filterZipList: List[(Long, Long)] = filterList.zip(filterList.tail)

        val pageIdToSumMap: collection.Map[Long, Long] = actionRDD.filter(action => filterList.contains(action.page_id)).map(action => (action.page_id,1)).countByKey()

        // 分子:连续两个页面点击次数 (pageId1-pageId2,count)
        //以sessionId为维度,结构转换 -> (sessionId,time,pageId)
        //以sessionId分组,以时间排序,转换结构 -> (pageId-pageId,count) 拉链/滑窗
        val sessionToPageFlows: RDD[ListBuffer[(String, Long)]] = actionRDD.map(action => (action.session_id, action.action_time, action.page_id)).groupBy(t => t._1).map {
            case (sessionId, datas) => {
                val pageIdSort: List[(String, String, Long)] = datas.toList.sortWith((t1, t2) => {
                    t1._2 < t2._2
                })
                val pageIdsList: List[Long] = pageIdSort.map(t => t._3)
                val listBuffer: ListBuffer[(String, Long)] = new mutable.ListBuffer[(String, Long)]()
                val iterator: Iterator[List[Long]] = pageIdsList.sliding(2)
                while (iterator.hasNext) {
                    val pageidTwos: List[Long] = iterator.next()
                    if(pageidTwos.size == 2){
                        listBuffer.append((pageidTwos(0) + "-" + pageidTwos(1), 1))
                    }
                }
                val filterListBuffer: ListBuffer[(String, Long)] = listBuffer.filter {
                    case (pageIds, num) => {
                        val listPageIds: Array[String] = pageIds.split("-")
                        filterZipList.contains((listPageIds(0).toLong, listPageIds(1).toLong))
                    }
                }
                filterListBuffer
                //                val pageIdsZip: List[(Long, Long)] = pageIdsList.zip(pageIdsList.tail)
            }
        }
        val pageflowRDD: RDD[(String, Long)] = sessionToPageFlows.flatMap(data => data)
        val pageflowToCountRDD: collection.Map[String, Long] = pageflowRDD.countByKey()

        pageflowToCountRDD.foreach{
            case (pageIds , count) => {
                val pageIdsList: Array[String] = pageIds.split("-")
                val sum: Long = pageIdToSumMap.getOrElse(pageIdsList(0).toLong,1L)
                println(pageIds + "=" + (count.toDouble / sum))
            }
        }

        sc.stop()
    }
}
