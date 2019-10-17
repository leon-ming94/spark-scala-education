package com.atguigu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @author shkstart
  */
object SparkStreaming_WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkStreaming_WordCount").setMaster("local[*]")
        //123456
//        11123456
        val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
        val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop112",999)
        val wordDStream: DStream[(String, Int)] = socketDStream.flatMap(line => {
            val words: mutable.ArrayOps[String] = line.split(" ")
            words.map((_, 1))
        })
        val wordCountDStream: DStream[(String, Int)] = wordDStream.reduceByKey(_+_)

        wordCountDStream.print()

        ssc.start()

        ssc.awaitTermination()

    }
}
