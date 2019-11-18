package com.atguigu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author shkstart
  */
object SparkBook01 {

    def main(args: Array[String]): Unit = {
        //TODO 获取上下文对象StreamingContext
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
        //TODO 从服务器的7777端口获取数据 
        val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop112", 7777)

        //TODO 以换行符分割 map 筛选出error的单词 filler
        val filtterDStream: DStream[String] = inputDStream.flatMap(_.split("\n")).filter("error".equals(_))

//        filtterDStream.foreachRDD(rdd => rdd.foreach(println))
        filtterDStream.print

        ssc.start()

        ssc.awaitTermination()
    }

}
