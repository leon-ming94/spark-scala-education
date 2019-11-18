package com.atguigu.warehouse.utils

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author shkstart
  */
object TextfileDatasetUtil {

    def getTextfileDataSet(spark: SparkSession, path: String) = {
        val sc: SparkContext = spark.sparkContext
        sc.hadoopConfiguration.addResource("nameservice1/core-site.xml")
        sc.hadoopConfiguration.addResource("nameservice1/hdfs-site.xml")
        import spark.implicits._

        //TODO read text file
        val dataSet: Dataset[String] = spark
                .read
                .textFile(path)
        dataSet
    }

//    def filterAndMapDataset[T](spark: SparkSession, dataset: Dataset[String], clazz: Class[T]) = {
//        import spark.implicits._
//
//        val filterDataSet: Dataset[String] = dataset.filter(line => JSONUtils.verifyIsJSON(line, clazz))
//
//        val resutlDataset: Dataset[T] = filterDataSet.map(line => {
//            val value: T = JSON.parseObject(line, clazz)
//            value
//        })
//
//        resutlDataset
//    }
}
