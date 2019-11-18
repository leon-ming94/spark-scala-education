package com.atguigu.warehouse.service

import com.atguigu.warehouse.bean.{DwsMember, QueryResult}
import com.atguigu.warehouse.dao.DwsMemberDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * @author shkstart
  */
object AdsMemberService {


    def queryDetailApi(spark: SparkSession, dt: String) = {
        import spark.implicits._
        import org.apache.spark.sql.functions._
        val result: Dataset[QueryResult] = DwsMemberDao.queryIdlMemberData(spark).as[QueryResult].where(s"dt = '${dt}'")

        //TODO 统计注册来源url人数
//        result.groupByKey(item => item.appregurl + "_" + item.dn + "_" + item.dt)
//                .mapGroups { case (key, itor) =>
//                    val strings: Array[String] = key.split("_")
//                    val appregurl: String = strings(0)
//                    val dn: String = strings(1)
//                    val dt: String = strings(2)
//                    val list: List[QueryResult] = itor.toList
//                    val count: Int = list.map(item => 1).sum
//                    (appregurl, count, dt, dn)
//                }.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")

        //TODO 统计各memberlevel等级 支付金额前三的用户
//        result
//                .mapPartitions { partition =>
//                    partition.map(item => ((item.appregurl, item.dt, item.dn), 1))
//                }
//                .groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
//                .map { item =>
//                    (item._1._1, item._1._2, item._1._3, item._2)
//                }.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")


        //TODO 统计所属网站人数
//        result.mapPartitions { partition =>
//            partition.map(item => ((item.sitename, item.dn, item.dt), 1))
//        }.groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _).map(item => ((item._1._1, item._1._2, item._1._3, item._2)))
//                .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_sitenamenum")

        //TODO 统计各memberlevel等级 支付金额前三的用户
        result
                .withColumn("rownum",
                    row_number()
                            .over(Window
                                    .partitionBy("sitename", "memberlevel")
                                    .orderBy(desc("paymoney"))))
                .where("rownum < 4")
                .orderBy("memberlevel", "rownum")
                .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname"
                    , "sitename", "vip_level", "paymoney", "rownum", "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")


    }


    /**
      * 统计各项指标 使用sql
      *
      * @param spark
      */
    def queryDetailSql(spark: SparkSession, dt: String) = {
        val appregurlCount = DwsMemberDao.queryAppregurlCount(spark, dt).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")
        val siteNameCount = DwsMemberDao.querySiteNameCount(spark, dt).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_sitenamenum")
        val regsourceNameCount = DwsMemberDao.queryRegsourceNameCount(spark, dt).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_regsourcenamenum")
        val adNameCount = DwsMemberDao.queryAdNameCount(spark, dt).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_adnamenum")
        val memberLevelCount = DwsMemberDao.queryMemberLevelCount(spark, dt).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_memberlevelnum")
        val vipLevelCount = DwsMemberDao.queryVipLevelCount(spark, dt).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_viplevelnum")
//        val top3MemberLevelPayMoneyUser = DwsMemberDao.getTop3MemberLevelPayMoneyUser(spark, dt)
        queryDetailApi(spark,dt)
    }
}
