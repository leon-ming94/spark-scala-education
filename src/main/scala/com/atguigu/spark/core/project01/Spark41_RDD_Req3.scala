package com.atguigu.spark.core.project01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author shkstart
  */
object Spark41_RDD_Req3 {
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

        //分子 分母 分子/分母
        //分母:转换结构 -> (pageId,1) -> 聚合
        //过滤掉无用数据
        val filterList: List[Long] = List(1L, 2L, 3L, 4L, 5L, 6L, 7L)

        val filterList2: List[(Long, Long)] = filterList.zip(filterList.tail)

        val pageIdsCount2: collection.Map[Long, Long] = actionRDD.filter(action => filterList.contains(action.page_id)).map(action => (action.page_id, 1)).countByKey()

        //分子:转换结构 -> (sessionId,time,pageId) -> 按sessionId分组 , 组内排序
        //转换结构 -> List(page) 滑窗/拉链 -> (pageId-pageId,1) -> 扁平化 , 聚合
        val pageIdsMapRDD: RDD[(String, String, Long)] = actionRDD.map(action => (action.session_id, action.action_time, action.page_id))
        val groupRDD: RDD[(String, Iterable[(String, String, Long)])] = pageIdsMapRDD.groupBy(t => t._1)
        val sessionToPageFlows: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(data => {
            val pageIdsList: List[(String, String, Long)] = data.toList.sortWith((t1, t2) => {
                t1._2 < t2._2
            })
            //1,2,3,4,5,6
            val pageIdsListToOne: List[Long] = pageIdsList.map(t => t._3)

            //(1,2) (2,3) (3,4)
            val pageIdsListToTwo: List[(Long, Long)] = pageIdsListToOne.zip(pageIdsListToOne.tail)

            val filterPageIdsToTwo: List[(Long, Long)] = pageIdsListToTwo.filter(t => filterList2.contains(t))

            //(1-2,1) (2-3,1)
            val pageIdsListToCount: List[(String, Int)] = filterPageIdsToTwo.map(t => (t._1 + "-" + t._2, 1))
            pageIdsListToCount
        })
        val pageFlowToOne: RDD[(String, Int)] = sessionToPageFlows.flatMap {
            case (sessionId, list) => {
                list
            }
        }

        val pageFlowToCount2: collection.Map[String, Long] = pageFlowToOne.countByKey()
        pageFlowToCount2.foreach {
            case (pageIds, count) => {
                val pageIdArray: Array[String] = pageIds.split("-")
                val value2: Long = pageIdsCount2.getOrElse(pageIdArray(0).toLong, 1L)
                println(pageIds + "=" + (count.toDouble / value2))
            }
        }

        sc.stop()
    }
}
