package com.atguigu.warehouse.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @author shkstart
  */
object QzCourseDao {

    /**
      *
      * @param spark
      * @param dt
      */
    def getQzSiteCourse(spark: SparkSession, dt: String) = {
        spark.sql(
            s"""
               |select
               |    sitecourseid,
               |    siteid,
               |    courseid,
               |    sitecoursename,
               |    coursechapter,
               |    sequence,
               |    status,
               |    creator as sitecourse_creator,
               |    createtime as sitecourse_createtime,
               |    helppaperstatus,
               |    servertype,
               |    boardid,
               |    showstatus,
               |    dt,
               |    dn
               |from dwd.dwd_qz_site_course
               |where dt = '${dt}'
            """.stripMargin)
    }

    /**
      *
      * @param spark
      * @param dt
      */
    def getQzCourse(spark: SparkSession, dt: String)={
        spark.sql(
            s"""
              |select
              |    courseid,
              |    majorid,
              |    coursename,
              |    isadvc,
              |    chapterlistid,
              |    pointlistid,
              |    dn
              |from dwd.dwd_qz_course
              |where dt = '${dt}'
            """.stripMargin)
    }

    /**
      *
      * @param spark
      * @param dt
      * @return
      */
    def getQzCourseEdusubject(spark: SparkSession, dt: String)={
        spark.sql(
            s"""
              |select
              |    courseeduid,
              |    edusubjectid,
              |    courseid,
              |    dn
              |from dwd.dwd_qz_course_edusubject
              |where dt = '${dt}'
            """.stripMargin)
    }
}
