package c.a.b.dw.q.dws.agg

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object QzWideTable {
  System.setProperty("HADOOP_USER_NAME","atguigu")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("qzwide").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")


    val date=20190722
    val dwdQzMemberPaperQuestion = ss.sql(s"select userid,paperviewid,chapterid,sitecourseid,questionid,majorid,useranswer,istrue,lasttime,opertype,paperid,spendtime,score,question_answer,dt,dn from dwd.dwd_qz_member_paper_question where dt=$date")
    val dwsQzChapter = ss.sql("select chapterid,chapterlistid,chaptername,sequence as chapter_sequence,status as chapter_status," +
      "chapter_courseid,chapternum,chapterallnum,outchapterid,chapterlistname,pointid,questype,pointname,pointyear" +
      ",chapter,excisenum,pointlistid,pointdescribe,pointlevel,typelist,point_score,thought,remid,pointnamelist," +
      s"typelistids,pointlist,dn from dws.dws_qz_chapter where dt=$date")

    val dwsQzCourse = ss.sql("select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence as course_sequence," +
      "status as course_status,sitecourse_creator,sitecourse_createtime,helppaperstatus,servertype,boardid,showstatus,majorid," +
      s"coursename,isadvc,chapterlistid,pointlistid,courseeduid,edusubjectid,dn from dws.dws_qz_course where dt=$date")
    val dwsQzMajor = ss.sql("select majorid,businessid,majorname,shortname,status as major_status,sequence  as major_sequence," +
      "major_creator,major_createtime,businessname,sitename,domain,multicastserver,templateserver,multicastgateway,multicastport," +
      s"dn from dws.dws_qz_major where dt=$date")


    val dwsQzPaper = ss.sql("select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest,contesttime," +
      "conteststarttime,contestendtime,contesttimelimit,dayiid,status as paper_status,paper_view_creator,paper_view_createtime," +
      "paperviewcatid,modifystatus,description,paperuse,testreport,centerid,sequence as paper_sequence,centername,centeryear," +
      "centertype,provideuser,centerviewtype,stage as paper_stage,papercatid,courseid,paperyear,suitnum,papername,totalscore,dn" +
      s" from dws.dws_qz_paper where dt=$date")


    val dwsQzQuestion = ss.sql("select questionid,parentid as question_parentid,questypeid,quesviewtype,content as question_content," +
      "answer as question_answer,analysis as question_analysis,limitminute as question_limitminute,score as question_score," +
      "splitscore,lecture,creator as question_creator,createtime as question_createtime,modifystatus as question_modifystatus," +
      "attanswer as question_attanswer,questag as question_questag,vanalysisaddr as question_vanalysisaddr,difficulty as question_difficulty," +
      "quesskill,vdeoaddr,description as question_description,splitscoretype as question_splitscoretype,dn " +
      s" from dws.dws_qz_question where dt=$date")


    val dwdQzMemberPaperQuestion2 = dwdQzMemberPaperQuestion.drop("paperid").withColumnRenamed("question_answer","user_question_answer")

    val dwsQzChapter2 = dwsQzChapter.drop("courseid")

    val dwsQzCourse2 = dwsQzCourse.withColumnRenamed("sitecourse_creator", "course_creator").withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid").drop("chapterlistid").drop("pointlistid")

    val dwsQzPaper2 = dwsQzPaper.drop("courseid")


    dwdQzMemberPaperQuestion2.join(dwsQzCourse2, Seq("sitecourseid", "dn")).
      join(dwsQzChapter2, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper2, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
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
        "question_splitscoretype", "user_question_answer", "dt", "dn"
      ).write.mode(SaveMode.Overwrite).insertInto("dws.dws_user_paper_detail")
  }
}
