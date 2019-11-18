package com.atguigu.warehouse.service

import com.atguigu.warehouse.bean.{DwsMember, DwsMember_Result, MemberZipper, MemberZipperResult}
import com.atguigu.warehouse.dao.DwdMemberDao
import org.apache.spark.sql._

/**
  * @author shkstart
  */
object DwsMemberService {

    /**
      * 使用sql方式 完成宽表\拉链表功能
      *
      * @param spark
      * @param time
      */
    def importMember(spark: SparkSession, time: String) = {
        import spark.implicits._
        spark.sql(
            s"""
               |select
               |    uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin),
               |    first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq),
               |    first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode),
               |    first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),
               |    first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename),
               |    first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level),
               |    min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level),
               |    first(vip_operator),dt,dn
               |from (
               |    select
               |        m.uid,m.ad_id,m.fullname,m.iconurl,m.lastlogin,m.mailaddr,m.memberlevel,
               |        m.password,money.paymoney,m.phone,m.qq,m.register,m.regupdatetime,m.unitname,m.userip,
               |        m.zipcode,m.dt,mreg.appkey,mreg.appregurl,mreg.bdp_uuid,mreg.createtime as reg_createtime,mreg.isranreg,mreg.regsource,
               |        mreg.regsourcename,bad.adname,bweb.siteid,bweb.sitename,bweb.siteurl,bweb.delete as site_delete,bweb.createtime as site_createtime,
               |        bweb.creator as site_creator,vip.vip_id,vip.vip_level,vip.start_time as vip_start_time,vip.end_time as vip_end_time,
               |        vip.last_modify_time as vip_last_modify_time,vip.max_free as vip_max_free,vip.min_free as vip_min_free,
               |        vip.next_level as vip_next_level,vip.operator as vip_operator,m.dn
               |    from dwd.dwd_member m
               |    left join dwd.dwd_member_regtype mreg on m.uid = mreg.uid and m.dn = mreg.dn
               |    left join dwd.dwd_base_ad bad on m.ad_id = bad.adid and m.dn = bad.dn
               |    left join dwd.dwd_base_website bweb on mreg.websiteid = bweb.siteid and mreg.dn = bweb.dn
               |    left join dwd.dwd_pcentermempaymoney money on m.uid = money.uid and m.dn = money.dn
               |    left join dwd.dwd_vip_level vip on money.vip_id = vip.vip_id and money.dn = vip.dn
               |    where m.dt = ${time}
               |)
               |group by uid,dn,dt
               |
            """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")


        //TODO 拉链表
        val dataHisSet: Dataset[MemberZipper] = spark.sql("select * from dws.dws_member_zipper").as[MemberZipper]
        val dataTodaySet: Dataset[MemberZipper] = spark.sql(
            s"""
               |select
               |    m.uid,sum(cast(m.paymoney as decimal(10,4))) as paymoney,max(vip.vip_level) as vip_level,
               |    from_unixtime(unix_timestamp('${time}','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,
               |    first(m.dn) as dn
               |from
               |    dwd.dwd_pcentermempaymoney m
               |join
               |    dwd.dwd_vip_level vip
               |on m.vip_id = vip.vip_id
               |and m.dn = vip.dn
               |where m.dt = ${time}
               |group by m.uid
            """.stripMargin).as[MemberZipper]

        //        dataHisSet.show()
        //        dataTodaySet.show()

        val unionDataSet: KeyValueGroupedDataset[String, MemberZipper] = dataHisSet.union(dataTodaySet).groupByKey(zip => zip.uid + "_" + zip.dn)
        val resultDataset: Dataset[MemberZipper] = unionDataSet
                .mapGroups { case (key, itor) =>
                    val list: List[MemberZipper] = itor.toList
                    val zippers: List[MemberZipper] = list.sortWith((m1, m2) => m1.start_time > m2.start_time)
                    if (zippers.length > 1) {
                        val last2Zipper: MemberZipper = zippers(zippers.length - 2)
                        val lastZipper: MemberZipper = zippers.last
                        if ("9999-12-31".equals(last2Zipper.end_time)) {
                            last2Zipper.end_time = lastZipper.start_time

                            lastZipper.paymoney = (BigDecimal.apply(lastZipper.paymoney) + BigDecimal(last2Zipper.paymoney)).toString()
                        }
                    }
                    zippers
                }
                .flatMap(list => list)
        resultDataset.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")
    }


    /**
      * API方式
      * @param spark
      * @param time
      */
    def importMemberByAPI(spark: SparkSession, time: String) = {
        //TODO 聚合宽表
        //TODO 先多表聚合 再分组查询
        import spark.implicits._ //隐式转换
        val dwdMember = DwdMemberDao.getDwdMember(spark).where(s"dt='${time}'") //主表用户表
        val dwdMemberRegtype = DwdMemberDao.getDwdMemberRegType(spark)
        val dwdBaseAd = DwdMemberDao.getDwdBaseAd(spark)
        val dwdBaseWebsite = DwdMemberDao.getDwdBaseWebSite(spark)
        val dwdPcentermemPaymoney = DwdMemberDao.getDwdPcentermemPayMoney(spark)
        val dwdVipLevel = DwdMemberDao.getDwdVipLevel(spark)

        val result: Dataset[DwsMember] = dwdMember.join(dwdMemberRegtype, Seq("uid", "dn"), "left")
                .join(dwdBaseAd, Seq("ad_id", "dn"), "left")
                .join(dwdBaseAd, dwdMember.col("ad_id") === dwdBaseAd.col("adid"), "left")
                .join(dwdBaseWebsite, Seq("siteid", "dn"), "left")
                .join(dwdPcentermemPaymoney, Seq("uid", "dn"), "left")
                .join(dwdVipLevel, Seq("vip", "dn"), "left")
                .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
                    , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
                    , "appregurl", "bdp_uuid", "reg_createtime", "isranreg", "regsource", "regsourcename", "adname"
                    , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
                    "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
                    , "vip_operator", "dt", "dn")
                .where(s"dt = '${time}'").as[DwsMember]
        //                .where($"dt" === s"'${time}'")
        result.groupByKey(member => member.uid + "_" + member.dn)
                .mapGroups { case (key, itor) =>
                    val strings: Array[String] = key.split("_")
                    val uid: Int = strings(0).toInt
                    val dn: String = strings(1)
                    val dwsMembers: List[DwsMember] = itor.toList
                    val paymoney = dwsMembers.filter(_.paymoney != null).map(_.paymoney).reduceOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString
                    val ad_id = dwsMembers.map(_.ad_id).head
                    val fullname = dwsMembers.map(_.fullname).head
                    val icounurl = dwsMembers.map(_.iconurl).head
                    val lastlogin = dwsMembers.map(_.lastlogin).head
                    val mailaddr = dwsMembers.map(_.mailaddr).head
                    val memberlevel = dwsMembers.map(_.memberlevel).head
                    val password = dwsMembers.map(_.password).head
                    val phone = dwsMembers.map(_.phone).head
                    val qq = dwsMembers.map(_.qq).head
                    val register = dwsMembers.map(_.register).head
                    val regupdatetime = dwsMembers.map(_.regupdatetime).head
                    val unitname = dwsMembers.map(_.unitname).head
                    val userip = dwsMembers.map(_.userip).head
                    val zipcode = dwsMembers.map(_.zipcode).head
                    val appkey = dwsMembers.map(_.appkey).head
                    val appregurl = dwsMembers.map(_.appregurl).head
                    val bdp_uuid = dwsMembers.map(_.bdp_uuid).head
                    val reg_createtime = dwsMembers.map(_.reg_createtime).head
                    val isranreg = dwsMembers.map(_.isranreg).head
                    val regsource = dwsMembers.map(_.regsource).head
                    val regsourcename = dwsMembers.map(_.regsourcename).head
                    val adname = dwsMembers.map(_.adname).head
                    val siteid = dwsMembers.map(_.siteid).head
                    val sitename = dwsMembers.map(_.sitename).head
                    val siteurl = dwsMembers.map(_.siteurl).head
                    val site_delete = dwsMembers.map(_.site_delete).head
                    val site_createtime = dwsMembers.map(_.site_createtime).head
                    val site_creator = dwsMembers.map(_.site_creator).head
                    val vip_id = dwsMembers.map(_.vip_id).head
                    val vip_level = dwsMembers.map(_.vip_level).max
                    val vip_start_time = dwsMembers.map(_.vip_start_time).min
                    val vip_end_time = dwsMembers.map(_.vip_end_time).max
                    val vip_last_modify_time = dwsMembers.map(_.vip_last_modify_time).max
                    val vip_max_free = dwsMembers.map(_.vip_max_free).head
                    val vip_min_free = dwsMembers.map(_.vip_min_free).head
                    val vip_next_level = dwsMembers.map(_.vip_next_level).head
                    val vip_operator = dwsMembers.map(_.vip_operator).head
                    DwsMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
                        phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
                        bdp_uuid, reg_createtime, isranreg, regsource, regsourcename, adname, siteid,
                        sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
                        vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
                        vip_next_level, vip_operator, time, dn)
                }.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")
    }
}
