package com.atguigu.spark.core.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author shkstart
  */
object Spark41_RDD_Req1 {
    def main(args: Array[String]): Unit = {
        //TODO 创建spark环境连接对象
        val conf: SparkConf = new SparkConf().setAppName("Spark41_RDD_Req1").setMaster("local[*]")
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


        //TODO 转换成CategoryCountInfo对象
        val flatMapRDD: RDD[CategoryCountInfo] = mapRDD.flatMap(action => {
            action match {
                case UserVisitAction(date: String,
                user_id: Long,
                session_id: String,
                page_id: Long,
                action_time: String,
                search_keyword: String,
                click_category_id: Long,
                click_product_id: Long,
                order_category_ids: String,
                order_product_ids: String,
                pay_category_ids: String,
                pay_product_ids: String,
                city_id: Long) if click_category_id != -1 => {
                    List(CategoryCountInfo(click_category_id.toString, 1, 0, 0))
                }
                case UserVisitAction(date: String,
                user_id: Long,
                session_id: String,
                page_id: Long,
                action_time: String,
                search_keyword: String,
                click_category_id: Long,
                click_product_id: Long,
                order_category_ids: String,
                order_product_ids: String,
                pay_category_ids: String,
                pay_product_ids: String,
                city_id: Long) if !order_category_ids.equals("null") => {
                    val cids: mutable.ArrayOps[String] = order_category_ids.split(",")
                    val list = new ListBuffer[CategoryCountInfo]()
                    cids.foreach(
                        id => {
                            list.append(CategoryCountInfo(id, 0, 1, 0))
                        }
                    )
                    list
                }
                case UserVisitAction(date: String,
                user_id: Long,
                session_id: String,
                page_id: Long,
                action_time: String,
                search_keyword: String,
                click_category_id: Long,
                click_product_id: Long,
                order_category_ids: String,
                order_product_ids: String,
                pay_category_ids: String,
                pay_product_ids: String,
                city_id: Long) if !pay_category_ids.equals("null") => {
                    val ids: mutable.ArrayOps[String] = pay_category_ids.split(",")
                    val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]()
                    ids.foreach(id => {
                        list.append(CategoryCountInfo(id, 0, 0, 1))
                    })
                    list
                }
                case _ => Nil
            }
        })

//        val mapFlatMapRDD: RDD[(String, (Long, Long, Long))] = flatMapRDD.map(data => {
//            (data.categoryId, (data.clickCount, data.orderCount, data.payCount))
//        })
//        val reduceRDD: RDD[(String, (Long, Long, Long))] = mapFlatMapRDD.reduceByKey((t1, t2) => {
//            (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
//        })
//        val result: Array[(String, (Long, Long, Long))] = reduceRDD.sortBy(t => (t._2._1,t._2._2,t._2._3),false).take(10)
//
//        result.foreach(println)

        val groupByRDD: RDD[(String, Iterable[CategoryCountInfo])] = flatMapRDD.groupBy(info => info.categoryId)

        val reduceRDD: RDD[CategoryCountInfo] = groupByRDD.map {
            case (ids, datas) => {
                datas.reduce((x, y) => {
                    x.clickCount = x.clickCount + y.clickCount
                    x.orderCount = x.orderCount + y.orderCount
                    x.payCount = x.payCount + y.payCount
                    x
                })
            }
        }
        val result: Array[CategoryCountInfo] = reduceRDD.sortBy(data => (data.clickCount,data.orderCount,data.payCount),false).take(10)
        result.foreach(println)


        //TODO 关闭资源
        sc.stop()
    }
}

/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的ID
  * @param session_id         Session的ID
  * @param page_id            某个页面的ID
  * @param action_time        动作的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的ID
  * @param click_product_id   某一个商品的ID
  * @param order_category_ids 一次订单中所有品类的ID集合
  * @param order_product_ids  一次订单中所有商品的ID集合
  * @param pay_category_ids   一次支付中所有品类的ID集合
  * @param pay_product_ids    一次支付中所有商品的ID集合
  * @param city_id            城市 id
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

case class CategoryCountInfo(categoryId: String,
                             var clickCount: Long,
                             var orderCount: Long,
                             var payCount: Long)
