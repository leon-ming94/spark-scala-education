package com.atguigu.spark.stream

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @author shkstart
  */
object SparkStreaming_kafkaSource {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkStreaming_kafkaSource").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

        val brokers: String = "hadoop112:9092,hadoop113:9092,hadoop114:9092"
        val topic: String = "first_1"
        val group: String = "bigdata"
        val deserialization: String = "org.apache.kafka.common.serialization.StringDeserializer"

        val kafkaParams: Map[String, String] = Map(ConsumerConfig.GROUP_ID_CONFIG -> group,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
        )

        val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))

        val wordsDStream: DStream[(String, Int)] = kafkaDStream.flatMap(t => {
            val words: mutable.ArrayOps[String] = t._2.split(" ")
            words.map((_, 1))
        })
        val wordCountDStream: DStream[(String, Int)] = wordsDStream.reduceByKey(_+_)

        wordCountDStream.print()

        ssc.start()

        ssc.awaitTermination()


    }
}
