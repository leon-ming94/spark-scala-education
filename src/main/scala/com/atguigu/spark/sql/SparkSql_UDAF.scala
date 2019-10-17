package com.atguigu.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @author shkstart
  */
object SparkSql_UDAF {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder().appName("SparkSql_UDAF").master("local[*]").getOrCreate()
        val df: DataFrame = spark.read.json("input/user.json")
        import spark.implicits._

//        val ds: Dataset[User] = df.as[User]
//        ds.createTempView("user")
        df.createTempView("user")
        val avgUdaf: avgUDAF = new avgUDAF()
        spark.udf.register("myAvg",avgUdaf)

        spark.sql("select myAvg(age) from user").show

        spark.stop()
    }
}

class avgUDAF extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
        StructType(Array(StructField("age",LongType)))
    }

    override def bufferSchema: StructType = {
        StructType(Array(StructField("total",LongType),StructField("count",LongType)))
    }

    override def dataType: DataType = {
        DoubleType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1L
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
        println(buffer.getLong(0) + ":" + buffer.getLong(1))
        buffer.getLong(0).toDouble / buffer.getLong(1)
    }
}

