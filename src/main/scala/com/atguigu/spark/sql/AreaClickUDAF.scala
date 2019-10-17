package com.atguigu.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._




/**
  * @author shkstart
  */
class AreaClickUDAF extends UserDefinedAggregateFunction {

//    override def inputSchema: StructType = {
//        StructType(Array(StructField("cName",StringType)))
//    }
//
//    override def bufferSchema: StructType = {
//        StructType(Array(StructField("map",MapType(StringType,LongType)),StructField("total",LongType)))
//
//    }
//
//    override def dataType: DataType = {
//        StringType
//    }
//
//    override def deterministic: Boolean = true
//
//    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//        buffer(0) = Map[String,Long]()
//        buffer(1) = 0L
//    }
//
//    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//        var map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
//        buffer(0) = map + (input.getString(0) -> (map.getOrElse(input.getString(0),0L) + 1L))
//        buffer(1) = buffer.getLong(1) + 1L
////        map(input.getString(0)) = map.getOrElse(input.getString(0),0L) + 1L
//    }
//
//    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//        var map1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
//        var map2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
//        map1.foldLeft(map2)((innerMap,data) => {
//            val key: String = data._1
//            val value: Long = data._2
//            innerMap + (key -> (innerMap.getOrElse(key,0L) + value))
////            innerMap(key) = innerMap.getOrElse(key,0L) + value
////            innerMap
//        })
//        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
//    }
//
//    override def evaluate(buffer: Row): Any = {
//        var s: StringBuffer = new StringBuffer()
//        val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
//        val total: Long = buffer.getLong(1)
//        val remarkList: List[(String, Long)] = map.toList.sortWith((t1,t2) => t1._2 > t2._2)
//        if ( remarkList.size > 2 ) {
//
//            val restList: List[(String, Long)] = remarkList.take(2)
//            val cityList: List[String] = restList.map {
//                case (cityName, clickCount) => {
//                    cityName + clickCount.toDouble / total * 100 + "%"
//                }
//            }
//            cityList.mkString(", ") + ", 其他 " + ( remarkList.tail.tail.map(_._2).sum / total * 100 + "%" )
//
//        } else {
//            val cityList: List[String] = remarkList.map {
//                case (cityName, clickCount) => {
//                    cityName + clickCount.toDouble / total * 100 + "%"
//                }
//            }
//            cityList.mkString(", ")
//        }


//        var s: StringBuffer = new StringBuffer()
//        val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
//        val list: List[(String, Long)] = map.toList.sortWith((t1,t2) => t1._2 > t2._2).take(2)
//        val sumList: Long = list.map(_._2).sum
//        val sum: Long = map.map(t => {
//            t._2
//        }).sum
//
//        val rest: Double = (sum - sumList).toDouble / sum
//
//        for (i <- 1 to list.size) {
//            val city: String = list(i)._1
//            val count: Long = list(i)._2
//            val percent: Double = count.toDouble / sum
//            s.append(city + " " + percent)
//            if(i != list.size){
//                s.append(", ")
//            }
//        }
//        if(rest != 0.0){
//            s.append(", 其他 " + rest)
//        }
//        s.toString
//    }
override def inputSchema: StructType = {
    StructType(Array(StructField("cityName", StringType)))
}

    // total, (北京 - 100，天津-50)
    override def bufferSchema: StructType = {
        StructType(Array(StructField("cityToCount", MapType(StringType, LongType)), StructField("total", LongType)))
    }

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Map[String, Long]()
        buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val cityName = input.getString(0)
        val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
        buffer(1) = buffer.getLong(1) + 1L
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

        val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
        val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)

        buffer1(0) = map1.foldLeft(map2){
            case ( map, (k, v) ) => {
                map + (k -> (map.getOrElse(k, 0L) + v))
            }
        }
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {

        // 获取城市点击次数，并根据点击次数进行排序取2条
        val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)

        val remarkList: List[(String, Long)] = map.toList.sortWith(
            (left, right) => {
                left._2 > right._2
            }
        )

        if ( remarkList.size > 2 ) {

            val restList: List[(String, Long)] = remarkList.take(2)
            val cityList: List[String] = restList.map {
                case (cityName, clickCount) => {
                    cityName + clickCount.toDouble / buffer.getLong(1) * 100 + "%"
                }
            }
            cityList.mkString(", ") + ", 其他 " + ( remarkList.tail.tail.map(_._2).sum / buffer.getLong(1) * 100 + "%" )

        } else {
            val cityList: List[String] = remarkList.map {
                case (cityName, clickCount) => {
                    cityName + clickCount.toDouble / buffer.getLong(1) * 100 + "%"
                }
            }
            cityList.mkString(", ")
        }


    }
}
