package com.atguigu.warehouse.etl

import com.alibaba.fastjson.JSON
import com.atguigu.warehouse.bean.Member
import com.atguigu.warehouse.utils.JSONUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author shkstart
  */
object Spark_ETL_Member {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .appName(this.getClass.getName)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()

        val sc: SparkContext = spark.sparkContext
        sc.hadoopConfiguration.addResource("nameservice1/core-site.xml")
        sc.hadoopConfiguration.addResource("nameservice1/hdfs-site.xml")


        import spark.implicits._
        //TODO read text file
        val dataSet: Dataset[String] = spark
                .read
                .textFile("hdfs://nameservice1/user/atguigu/ods/member.log")
        //        dataSet.show()
        //TODO filter
        val filterDateset: Dataset[String] = dataSet.filter(line => {
            JSONUtils.verifyIsJSON(line, classOf[Member])
        })

        //TODO ETL
        //TODO map
        val mapDataset: Dataset[Member] = filterDateset.map(line => {
            val member: Member = JSON.parseObject(line, classOf[Member])
            val phone: String = member.phone
            val prefix: String = phone.substring(0, 3)
            val suffix: String = phone.substring(7)
            member.phone = prefix + "****" + suffix
            member
        })


        //TODO write to hive table
        mapDataset
                .write
                .format("hive")
                .mode("overwrite")
                .insertInto("dwd.dwd_member")
    }
}
