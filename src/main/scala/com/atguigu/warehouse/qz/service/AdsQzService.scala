package com.atguigu.warehouse.qz.service

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author shkstart
  */
object AdsQzService {

    def getTargetApi(spark: SparkSession, dt: String) = {
        import org.apache.spark.sql.functions._
        val result: DataFrame = spark.sql(
            s"""
                select
                    paperviewid,
                    paperviewname,
                    score,
                    spendtime,
                    dt,
                    dn
                from dws.dws_user_paper_detail
             """.stripMargin).cache()
        //TODO 求个试卷平时耗时 平均分
        result
                .where(s"dt = '${dt}'")
                .groupBy("paperviewid", "paperviewname", "dt", "dn")
                .agg(avg("score").cast("decimal(4,1)").as("avgscore"),
                    avg("spendtime").cast("decimal(4,1)").as("avgspendtime"))
                .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_avgtimeandscore")

        //TODO 求最高分 最低分
        result
                .where(s"dt = '${dt}'")
                .groupBy("paperviewid", "paperviewname", "dt", "dn")
                .agg(max("score").as("maxscore"),
                    min("score").as("minscore"))
                //TODO ???为什么要select
                .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")
        result.unpersist()

        //TODO 前三用户详情
        val top3Result: DataFrame = spark.sql("select *from dws.dws_user_paper_detail")
        top3Result
                .where(s"dt = '${dt}'")
                .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
                    , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
                .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
                .where("rk < 4")
                .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
                    , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
                .coalesce(5).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")



        //TODO 倒数前三
        spark.sql("select *from dws.dws_user_paper_detail")
                .where(s"dt = '${dt}'")
                .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
                    , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
                .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(asc("score"))))
                .where("rk < 4")
                .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
                    , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
                .coalesce(5).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")
//        top3Result.unpersist()

        //TODO 分段用户id
        val resultSegment: DataFrame = spark.sql(
            s"""
                select
                    paperviewid,
                    paperviewname,
                    score,
                    userid,
                    dt,
                    dn
                from dws.dws_user_paper_detail
             """.stripMargin).cache()

        resultSegment
                .where(s"dt ='${dt}'")
                .select(col("paperviewid"),
                    col("paperviewname"),
                    when(col("score") >= 0 && col("score") < 20, "0-20")
                            .when(col("score") >= 20 && col("score") < 40, "20-40")
                            .when(col("score") >= 40 && col("score") < 60, "40-60")
                            .when(col("score") >= 60 && col("score") < 80, "60-80")
                            .when(col("score") >= 80 && col("score") <= 100, "80-100")
                            .as("score_segment"),
                    col("userid"),
                    col("dt"),
                    col("dn"))
                .groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
                .agg(concat_ws("|", collect_list(col("userid").cast("string").as("userids"))).as("userids"))
                .select("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")

        //TODO 及格率
        resultSegment
                .where(s"dt ='${dt}'")
                .groupBy("paperviewid", "paperviewname", "dt", "dn")
                .agg(sum(when(col("score") >= 60, 1)).as("passcount"),
                    sum(when(col("score") < 60, 1)).as("unpasscount"))
                .select("paperviewid", "paperviewname", "unpasscount", "passcount", "dt", "dn")
                .withColumn("rate", (col("passcount") / (col("unpasscount") + col("passcount"))).cast("decimal(4,2)"))
                .select("paperviewid", "paperviewname", "unpasscount", "passcount", "rate", "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")
        resultSegment.unpersist()


        //TODO 错题率
        val questionResult: DataFrame = spark.sql(
            s"""
                select
                    questionid,
                    user_question_answer,
                    dt,
                    dn
                from dws.dws_user_paper_detail
            """.stripMargin)

        questionResult
                .where(s"dt = '${dt}'")
                .groupBy("questionid", "dt", "dn")
                .agg(sum(when(col("user_question_answer") === "1", 1)).as("rightcount"),
                    sum(when(col("user_question_answer") === "0", 1)).as("errcount"))
                .select("questionid", "errcount", "rightcount", "dt", "dn")
                .withColumn("rate", (col("errcount") / (col("errcount") + col("rightcount"))).cast("decimal(4,2)"))
                .select("questionid", "errcount", "rightcount", "rate", "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_question_detail")


    }


}
