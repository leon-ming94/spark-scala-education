package com.atguigu.warehouse.qz.service

import com.atguigu.warehouse.qz.dao._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * @author shkstart
  */
object DwsQzService {

    def saveDwsUserPaperDetail(spark: SparkSession, dt: String)= {
//        val memberPaperQuestion: DataFrame = UserPaperDetailDao.getDwdQzMemberPaperQuestion(spark, dt)
//                .drop("paperid")
//                .withColumnRenamed("question_answer", "user_question_answer")
//        val course: DataFrame = UserPaperDetailDao.getDwsQzCourse(spark, dt)
//                .withColumnRenamed("sitecourse_creator", "course_creator")
//                .withColumnRenamed("sitecourse_createtime", "course_createtime")
//                .drop("majorid")
//                .drop("chapterlistid")
//                .drop("pointlistid")
//        val question: DataFrame = UserPaperDetailDao.getDwsQzQuestion(spark, dt)
//        val paper: DataFrame = UserPaperDetailDao.getDwsQzPaper(spark, dt).drop("courseid")
//        val chapter: DataFrame = UserPaperDetailDao.getDwsQzChapter(spark, dt).drop("courseid")
//        val major: DataFrame = UserPaperDetailDao.getDwsQzMajor(spark, dt)
//
//        memberPaperQuestion.join(chapter, Seq("chapterid", "dn"))
//                .join(course, Seq("sitecourseid", "dn"))
//                .join(major, Seq("majorid", "dn"))
//                .join(paper, Seq("paperviewid", "dn"))
//                .join(question, Seq("questionid", "dn"))
//                .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
//                    "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
//                    "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
//                    , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
//                    "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
//                    , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
//                    "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
//                    "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
//                    "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
//                    "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
//                    "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
//                    "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
//                    "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
//                    "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
//                    "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
//                    "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
//                    "question_splitscoretype", "user_question_answer", "dt", "dn")
//                //.coalesce(5)
//                .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")

        val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(spark, dt).drop("paperid")
                .withColumnRenamed("question_answer", "user_question_answer")
        val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(spark, dt).drop("courseid")
        val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(spark, dt).withColumnRenamed("sitecourse_creator", "course_creator")
                .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
                .drop("chapterlistid").drop("pointlistid")
        val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(spark, dt)
        val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(spark, dt).drop("courseid")
        val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(spark, dt)
        dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
                join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
                .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
                .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
                    "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
                    "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
                    , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
                    "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
                    , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
                    "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
                    "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
                    "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
                    "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
                    "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
                    "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
                    "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
                    "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
                    "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
                    "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
                    "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(5)
                .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")
    }

    /**
      *
      * @param spark
      * @param dt
      */
    def saveDwsQzQuestionTpe(spark: SparkSession, dt: String) = {
        val question: DataFrame = QzQuestionDao.getQzQuestion(spark, dt)
        val questionType: DataFrame = QzQuestionDao.getQzQuestionType(spark, dt)

        question.join(questionType, Seq("questypeid", "dn"))
                .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
                    , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
                    , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
                    "remark", "splitscoretype", "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")
    }

    /**
      *
      * @param spark
      * @param dt
      */
    def saveDwsQzPaper(spark: SparkSession, dt: String) = {
        val paperView: DataFrame = QzPaperDao.getDwdQzPaperView(spark, dt)
        val centerPaper: DataFrame = QzPaperDao.getDwdQzCenterPaper(spark, dt)
        val paper: DataFrame = QzPaperDao.getDwdQzPaper(spark, dt)
        val center: DataFrame = QzPaperDao.getDwdQzCenter(spark, dt)

        paperView.join(centerPaper, Seq("paperviewid", "dn"))
                .join(paper, Seq("paperid", "dn"))
                .join(center, Seq("centerid", "dn"))
                .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
                    , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
                    "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
                    "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
                    "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
                    "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
    }

    /**
      *
      * @param spark
      * @param dt
      */
    def saveDwsQzMajor(spark: SparkSession, dt: String) = {
        val dwdQzMajor = QzMajorDao.getQzMajor(spark, dt)
        val dwdQzWebsite = QzMajorDao.getQzWebsite(spark, dt)
        val dwdQzBusiness = QzMajorDao.getQzBusiness(spark, dt)
        val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
                .join(dwdQzBusiness, Seq("businessid", "dn"))
                .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
                    "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
                    "multicastgateway", "multicastport", "dt", "dn")
        result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
    }

    /**
      *
      * @param spark
      * @param dt
      */
    def saveDwsQzCourse(spark: SparkSession, dt: String) = {
        val siteCourse: DataFrame = QzCourseDao.getQzSiteCourse(spark, dt)
        val course: DataFrame = QzCourseDao.getQzCourse(spark, dt)
        val CourseEdusubject: DataFrame = QzCourseDao.getQzCourseEdusubject(spark, dt)

        siteCourse.join(course, Seq("courseid", "dn"))
                .join(CourseEdusubject, Seq("courseid", "dn"))
                .select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
                    "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
                    "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
                    , "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")

    }

    /**
      * 章节表数据
      *
      * @param spark
      * @param dt
      * @return
      */
    def saveDwsQzChapter(spark: SparkSession, dt: String) = {
        val chapter: DataFrame = QzChapterDao.getDwdQzChapter(spark, dt)
        val chapterList: DataFrame = QzChapterDao.getDwdQzChapterList(spark, dt)
        val point: DataFrame = QzChapterDao.getDwdQzPoint(spark, dt)
        val pointQuestion: DataFrame = QzChapterDao.getDwdQzPointQuestion(spark, dt)

        chapter.join(chapterList, Seq("chapterlistid", "dn"))
                .join(point, Seq("chapterid", "dn"))
                .join(pointQuestion, Seq("pointid","dn"))
                .select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "showstatus",
                    "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
                    "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
                    "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
    }

}
