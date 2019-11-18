package com.atguigu.warehouse.utils

import com.alibaba.fastjson.JSON

/**
  * @author shkstart
  */
object JSONUtils {
    def verifyIsJSON[T](content: String, clazz: Class[T]): Boolean = {
        val result: Any = try {
            val value: T = JSON.parseObject(content, clazz)
            value
        } catch {
            case ex: Exception => null
        }

        if (null != result) {
            return true
        }
        return false
    }
}
