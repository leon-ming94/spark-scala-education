package com.atguigu.warehouse.dao

import org.apache.spark.sql.SparkSession

/**
  * @author shkstart
  */
object DwsMemberDao {

    /**
      * 查询用户宽表数据
      *
      * @param sparkSession
      * @return
      */
    def queryIdlMemberData(sparkSession: SparkSession) = {
        sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
                "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ")
    }

    /**
      * 统计注册来源url人数
      * @param spark
      * @param time
      * @return
      */
    def queryAppregurlCount(spark: SparkSession, time: String) = {
        spark.sql(
            s"""
              |select
              |    m.appregurl,
              |    count(m.uid),
              |    m.dn,
              |    m.dt
              |from dws.dws_member m
              |where dt = '${time}'
              |group by appregurl,dt,dn
            """.stripMargin)
    }


    /**
      * 统计所属网站人数
      * @param spark
      * @param time
      * @return
      */
    def querySiteNameCount(spark: SparkSession, time: String)={
        spark.sql(
            s"""
              |select
              |    m.sitename,
              |    count(m.uid),
              |    dn,
              |    dt
              |from  dws.dws_member m
              |where m.dt = '${time}'
              |group by m.sitename,dt,dn
            """.stripMargin)
    }

    /**
      * 统计所属来源人数
      * @param spark
      * @param time
      * @return
      */
    def queryRegsourceNameCount(spark: SparkSession,time:String)={
        spark.sql(
            s"""
              |select
              |    m.regsourcename,
              |    count(m.uid),
              |    dn,
              |    dt
              |from  dws.dws_member m
              |where m.dt = '${time}'
              |group by m.regsourcename,dt,dn
            """.stripMargin)
    }

    /**
      * 统计通过各广告注册的人数
      * @param spark
      * @param time
      * @return
      */
    def queryAdNameCount(spark: SparkSession,time:String)={
        spark.sql(
            s"""
              |select
              |    m.adname,
              |    count(m.uid),
              |    dn,
              |    dt
              |from  dws.dws_member m
              |where m.dt = '${time}'
              |group by m.adname,dt,dn
            """.stripMargin)
    }

    /**
      * 统计各用户等级人数
      * @param spark
      * @param time
      * @return
      */
    def queryMemberLevelCount(spark: SparkSession,time:String) ={
        spark.sql(
            s"""
              |select
              |    m.memberlevel,
              |    count(m.uid),
              |    dn,
              |    dt
              |from  dws.dws_member m
              |where m.dt = '${time}'
              |group by m.memberlevel,dt,dn
            """.stripMargin)
    }

    /**
      * 统计各用户vip等级人数
      * @param spark
      * @param time
      * @return
      */
    def queryVipLevelCount(spark: SparkSession,time:String)={
        spark.sql(
            s"""
              |select
              |    m.vip_level,
              |    count(m.uid),
              |    dn,
              |    dt
              |from  dws.dws_member m
              |where m.dt = '${time}'
              |group by m.vip_level,dt,dn
            """.stripMargin)
    }

    /**
      * 统计各memberlevel等级 支付金额前三的用户
      * @param spark
      * @param time
      * @return
      */
    def getTop3MemberLevelPayMoneyUser(spark: SparkSession,time:String)={
        spark.sql(
            s"""
              |select
              |    *
              |from (
              |    select
              |        uid,
              |        ad_id,
              |        memberlevel,
              |        register,
              |        appregurl,
              |        regsource,
              |        regsourcename,
              |        adname,
              |        siteid,
              |        sitename,
              |        vip_level,
              |        cast(paymoney as decimal(10,4)),
              |        dn,
              |        row_number() over(partition by sitename,memberlevel order by cast(paymoney as decimal(10,4)) desc) rk
              |    from dws.dws_member m
              |    where dt='${time}'
              |)t1
              |where rk < 3
            """.stripMargin)
    }

}
