package com.atguigu.warehouse.bean

/**
  * @author shkstart
  */
case class MemberRegtype(uid: Int,
                         appkey: String,
                         appregurl: String,
                         bdp_uuid: String,
                         createtime: String,
                         isranreg: String,
                         regsource: String,
                         var regsourcename: String,
                         websiteid: Int,
                         dt: String,
                         dn: String) {
    regsourcename = regsource match {
        case "1" => "PC"
        case "2" => "MOBILE"
        case "3" => "APP"
        case "4" => "WECHAT"
        case _ => "type illegal"
    }
}
