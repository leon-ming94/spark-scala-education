package com.atguigu.warehouse.bean

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.utils.JSONUtils

/**
  * @author shkstart
  */
case class Member(
                         uid: Int,
                         ad_id: Int,
                         birthday: String,
                         email: String,
                         fullname: String,
                         iconurl: String,
                         lastlogin: String,
                         mailaddr: String,
                         memberlevel: String,
                         password: String,
                         paymoney: String,
                         var phone: String,
                         qq: String,
                         register: String,
                         regupdatetime: String,
                         unitname: String,
                         userip: String,
                         zipcode: String,
                         dt: String,
                         dn: String
                 )

object Member{
    def main(args: Array[String]): Unit = {
        val line: String = "{\"appkey\":\"-\",\"appregurl\":\"http:www.webA.com/sale/register/index.html\",\"bdp_uuid\":\"-\",\"createtime\":\"2017-03-30\",\"dn\":\"webA\",\"domain\":\"-\",\"dt\":\"20190722\",\"isranreg\":\"-\",\"regsource\":\"0\",\"uid\":\"0\",\"websiteid\":\"4\"}"
        val member: MemberRegtype = JSON.parseObject(line,classOf[MemberRegtype])
        println(JSONUtils.verifyIsJSON(line, classOf[MemberRegtype]))
//        println(JSONUtils.verifyIsJSON(line, classOf[Member]))
//        println(member)
    }
}