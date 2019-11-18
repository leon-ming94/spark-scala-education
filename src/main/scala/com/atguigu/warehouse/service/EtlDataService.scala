package com.atguigu.warehouse.service

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.bean._
import com.atguigu.warehouse.utils.JSONUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author shkstart
  */
object EtlDataService {

    /**
      * etl用户注册信息
      *
      * @param ssc
      * @param spark
      */
    def etlMemberRegtypeLog(ssc: SparkContext, spark: SparkSession) = {
        import spark.implicits._
        val dataRDD: RDD[String] = ssc.textFile("/user/atguigu/ods/memberRegtype.log")
        val dataFrame: DataFrame = dataRDD
                .filter(line => JSONUtils.verifyIsJSON(line, classOf[MemberRegtype]))
                .mapPartitions(partition =>
                    partition.map { line => {
                        val memberRegtype: MemberRegtype = JSON.parseObject(line, classOf[MemberRegtype])
                        memberRegtype
                    }
                    }
                )
                .coalesce(1)
                .toDF()

        dataFrame.write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
    }

    /**
      * etl用户表数据
      *
      * @param ssc
      * @param spark
      */
    def etlMemberLog(ssc: SparkContext, spark: SparkSession) = {
        import spark.implicits._
        val dataRDD: RDD[String] = ssc.textFile("/user/atguigu/ods/member.log")
        val dataFrame: DataFrame = dataRDD
                .filter(line => JSONUtils.verifyIsJSON(line, classOf[Member]))
                .mapPartitions { partition =>
                    partition.map {
                        line => {
                            val member: Member = JSON.parseObject(line, classOf[Member])
                            val phone: String = member.phone
                            val prefix: String = phone.substring(0, 3)
                            val suffix: String = phone.substring(7)
                            member.phone = prefix + "****" + suffix
                            member
                        }
                    }
                }
                .coalesce(1)
                .toDF()

        dataFrame.write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
    }

    /**
      * 导入广告表基础数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        val dataRDD: RDD[String] = ssc.textFile("/user/atguigu/ods/baseadlog.log")
        //隐式转换
        dataRDD
                .filter(item => {
                    JSONUtils.verifyIsJSON(item, classOf[BaseAd])
                })
                .mapPartitions(partition => {
                    partition.map(item => {
                        JSON.parseObject(item, classOf[BaseAd])
                    })
                }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
    }

    /**
      * 导入网站表基础数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._ //隐式转换
        val dataRDD: RDD[String] = ssc.textFile("/user/atguigu/ods/baswewebsite.log")
        dataRDD
                .filter(item => {
                    JSONUtils.verifyIsJSON(item, classOf[BaseWebsite])
                })
                .mapPartitions(partition => {
                    partition.map(item => {
                        JSON.parseObject(item, classOf[BaseWebsite])
                    })
                }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_base_website")
    }


    /**
      * 导入用户付款信息
      *
      * @param ssc
      * @param sparkSession
      */
    def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._ //隐式转换
        ssc.textFile("/user/atguigu/ods/pcentermempaymoney.log").filter(item => {
            JSONUtils.verifyIsJSON(item, classOf[PcentermemPaymoney])
        }).mapPartitions(partition => {
            partition.map(item => {
                JSON.parseObject(item, classOf[PcentermemPaymoney])
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
    }

    /**
      * 导入用户vip基础数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._ //隐式转换
        ssc.textFile("/user/atguigu/ods/pcenterMemViplevel.log").filter(item => {
            JSONUtils.verifyIsJSON(item, classOf[VipLevel])
        }).mapPartitions(partition => {
            partition.map(item => {
                JSON.parseObject(item, classOf[VipLevel])
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_vip_level")
    }
}
