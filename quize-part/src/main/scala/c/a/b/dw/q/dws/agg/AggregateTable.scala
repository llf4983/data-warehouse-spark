package c.a.b.dw.q.dws.agg

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object AggregateTable {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    val conf = new SparkConf().setAppName("agg").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")


    val date=20190722
//    ss.sql("select t1.chapterid,t1.chapterlistid,t1.chaptername,t1.sequence,t1.showstatus,t2.status," +
//      "t1.creator as chapter_creator,t1.createtime as chapter_createtime,t1.courseid as chapter_courseid,t1.chapternum," +
//      "t2.chapterallnum,t1.outchapterid,t2.chapterlistname,t3.pointid,t3.questionid,t3.questype,t4.pointname,t4.pointyear," +
//      "t4.chapter,t4.excisenum,t4.pointlistid,t4.pointdescribe,t4.pointlevel,t4.typelist,t4.score as point_score,t4.thought," +
//      "t4.remid ,t4.pointnamelist ,t4.typelistids ,t4.pointlist ,t4.dt ,t4.dn from dwd.dwd_qz_chapter t1 inner join " +
//      "dwd.dwd_qz_chapter_list t2 on t1.chapterlistid=t2.chapterlistid and t1.dn=t2.dn inner join dwd.dwd_qz_point t4 " +
//      s"on t1.chapterid=t4.chapterid and t1.dn=t4.dn inner join dwd.dwd_qz_point_question t3 on t4.pointid=t3.pointid and t3.dn=t4.dn where t1.dt=$date"
//    ).show()
//    ss.sql("select * from dwd.dwd_qz_chapter limit 10").show()
    import org.apache.spark.sql.functions._

    val chapter = ss.sql(s"select chapterid,chapterlistid,chaptername,sequence,showstatus,creator as chapter_creator,createtime as chapter_createtime,courseid as chapter_courseid,chapternum,outchapterid,dn from dwd.dwd_qz_chapter where dt=$date")
    val chapterList=ss.sql(s"select chapterlistid,status,chapterallnum,chapterlistname,dn from dwd.dwd_qz_chapter_list where dt=$date")
    val pointQuestion = ss.sql(s"select pointid,questionid,questype,dn from dwd.dwd_qz_point_question where dt=$date")
    val point = ss.sql(s"select chapterid,pointid,pointname,pointyear,chapter,excisenum,pointlistid,pointdescribe,pointlevel,typelist,score as point_score,thought,remid,pointnamelist,typelistids,pointlist,dt,dn from dwd.dwd_qz_point where dt=$date")


    //dws_qz_chapter
    chapter.join(chapterList,Seq("chapterlistid","dn"))
      .join(point,Seq("chapterid","dn"))
      .join(pointQuestion,Seq("pointid","dn"))
      .select("chapterid","chapterlistid","chaptername","sequence","showstatus","status","chapter_creator","chapter_createtime",
        "chapter_courseid","chapternum","chapterallnum","outchapterid","chapterlistname","pointid","questionid","questype","pointname",
        "pointyear","chapter","excisenum","pointlistid","pointdescribe","pointlevel","typelist","point_score","thought","remid" ,"pointnamelist" ,
        "typelistids","pointlist","dt","dn"
      ).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_chapter")

    val siteCourse = ss.sql(s"select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence,status,creator as sitecourse_creator,createtime as sitecourse_createtime,helppaperstatus,servertype,boardid,showstatus,dn from dwd.dwd_qz_site_course  where dt=$date")
    val course = ss.sql(s"select courseid,majorid,coursename,isadvc,chapterlistid,pointlistid,dn from dwd.dwd_qz_course where dt=$date")
    val courseEdusubject = ss.sql(s"select courseid,courseeduid,edusubjectid,dt,dn from dwd.dwd_qz_course_edusubject where dt=$date")


    //dws_qz_course
    siteCourse.join(course,Seq("courseid","dn")).join(courseEdusubject,Seq("courseid","dn")
    ).select("sitecourseid","siteid","courseid","sitecoursename","coursechapter","sequence","status","sitecourse_creator","sitecourse_createtime","helppaperstatus","servertype","boardid","showstatus","majorid","coursename","isadvc","chapterlistid","pointlistid","courseeduid","edusubjectid","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_course")


    //dws.dws_qz_major
    val major = ss.sql(s"select majorid,businessid,siteid,majorname,shortname,status,sequence,creator as major_creator,createtime as major_createtime,dn from dwd.dwd_qz_major where dt=$date")
    val business = ss.sql(s"select businessid,businessname,dn from dwd.dwd_qz_business where dt=$date")
    val website = ss.sql(s"select siteid,sitename,domain,multicastserver,templateserver,multicastgateway,multicastport,dt,dn from dwd.dwd_qz_website where dt=$date")

    major.join(website,Seq("siteid","dn")).join(business,Seq("businessid","dn")
    ).select("majorid","businessid","siteid","majorname","shortname","status","sequence","major_creator","major_createtime","businessname","sitename","domain","multicastserver","templateserver","multicastgateway","multicastport","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_major")


    //dws.dws_qz_paper
    val paperView = ss.sql(s"select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest,contesttime,conteststarttime,contestendtime,contesttimelimit,dayiid,status,creator as paper_view_creator,createtime as paper_view_createtime,paperviewcatid,modifystatus,description,paperuse,paperdifficult,testreport,paperuseshow,dn from dwd.dwd_qz_paper_view where dt=$date")
    val centerPaper = ss.sql(s"select paperviewid,sequence,centerid,dn from dwd.dwd_qz_center_paper where dt=$date")
    val center = ss.sql(s"select centerid,centername,centeryear,centertype,provideuser,centerviewtype,stage,dn from dwd.dwd_qz_center where dt=$date")
    val paper = ss.sql(s"select paperid,papercatid,courseid,paperyear,suitnum,papername,totalscore,chapterid,chapterlistid,dt,dn from dwd.dwd_qz_paper where dt=$date")
    paperView.join(centerPaper,Seq("paperviewid", "dn"),"left").join(center,Seq("centerid","dn"),"left").join(paper,Seq("paperid", "dn")
    ).select("paperviewid","paperid","paperviewname","paperparam","openstatus","explainurl","iscontest","contesttime","conteststarttime","contestendtime","contesttimelimit","dayiid","status","paper_view_creator","paper_view_createtime","paperviewcatid","modifystatus","description","paperuse","paperdifficult","testreport","paperuseshow","centerid","sequence","centername","centeryear","centertype","provideuser","centerviewtype","stage","papercatid","courseid","paperyear","suitnum","papername","totalscore","chapterid","chapterlistid","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_paper")

    //dws.dws_qz_question
    val question = ss.sql(s"select questionid,parentid,questypeid,quesviewtype,content,answer,analysis,limitminute,score,splitscore,status,optnum,lecture,creator,createtime,modifystatus,attanswer,questag,vanalysisaddr,difficulty,quesskill,vdeoaddr,dn from dwd.dwd_qz_question where dt=$date")
    val questionType = ss.sql(s"select questypeid,viewtypename,description,papertypename,splitscoretype,dt,dn from dwd.dwd_qz_question_type where dt=$date")

    question.join(questionType,Seq("questypeid","dn")
    ).select("questionid","parentid","questypeid","quesviewtype","content","answer","analysis","limitminute","score","splitscore","status","optnum","lecture","creator","createtime","modifystatus","attanswer","questag","vanalysisaddr","difficulty","quesskill","vdeoaddr","viewtypename","description","papertypename","splitscoretype","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_question")

  }
}
