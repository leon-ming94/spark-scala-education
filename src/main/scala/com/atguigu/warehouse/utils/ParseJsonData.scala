package com.atguigu.warehouse.utils

import com.alibaba.fastjson.JSON


object ParseJsonData {

    //    public static JSONObject getJsonData(String data) {
    //        try {
    //            return JSONObject.parseObject(data);
    //        } catch (Exception e) {
    //            return null;
    //        }
    //    }
    def getJsonData(data: String) = {
        try {
            JSON.parseObject(data)
        } catch {
            case ex: Exception => null
        }
    }
}
