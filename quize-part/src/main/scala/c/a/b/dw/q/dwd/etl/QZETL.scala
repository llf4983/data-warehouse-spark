package c.a.b.dw.q.dwd.etl

import c.a.b.dw.q.dwd.bean.{QzMemberPaperQuestion, QzPaper, QzPoint, QzQuestion}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object QZETL {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val conf = new SparkConf().setAppName("qzetl").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    //  ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set hive.exec.compress.output=true")
    //  ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapred.output.compress=true")
    //  ss.sql("set mapred.output.compress=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    //  ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    //  ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    //  ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    val qzWebsite = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzWebsite.log")
    val qzPoint = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzPoint.log")
    val qzSiteCourse = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzSiteCourse.log")
    val qzQuestionType = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzQuestionType.log")
    val qzQuestion = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzQuestion.log")
    val qzPointQuestion = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzPointQuestion.log")
    val qzPaperView = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzPaperView.log")
    val qzPaper = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzPaper.log")
    val qzMemberPaperQuestion = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzMemberPaperQuestion.log")
    val qzMajor = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzMajor.log")
    val qzCourseEduSubject = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzCourseEduSubject.log")
    val qzCourse = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzCourse.log")
    val qzChapterList = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzChapterList.log")
    val qzChapter = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzChapter.log")
    val qzCenterPaper = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzCenterPaper.log")
    val qzCenter = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzCenter.log")
    val qzBusiness = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/QzBusiness.log")

    import ss.implicits._
    import org.apache.spark.sql.functions._
    qzPoint.as[QzPoint].map(ele=>{
      ele.score=BigDecimal.apply(ele.score).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()

      ele
    }).withColumn("typelistids",lit("-")).select("pointid","courseid","pointname","pointyear","chapter","creator","createtime","status","modifystatus","excisenum","pointlistid","chapterid","sequece","pointdescribe","pointlevel","typelist","score","thought","remid","pointnamelist","typelistids","pointlist","dt","dn"
    )//.write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point")

    qzPaper.as[QzPaper].map(ele=>{
      ele.totalscore=BigDecimal.apply(ele.totalscore).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
      ele
    }).select("paperid","papercatid","courseid","paperyear","chapter","suitnum","papername","status","creator","createtime","totalscore","chapterid","chapterlistid","dt","dn"
    )//.write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_paper")


    qzQuestion.as[QzQuestion].map(ele=>{
      ele.score=BigDecimal.apply(ele.score).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
      ele.splitscore=BigDecimal.apply(ele.splitscore).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
      ele
    }).select("questionid","parentid","questypeid","quesviewtype","content","answer","analysis","limitminute","score","splitscore","status","optnum","lecture","creator","createtime","modifystatus","attanswer","questag","vanalysisaddr","difficulty","quesskill","vdeoaddr","dt","dn"
    ) //.write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_question")

    qzMemberPaperQuestion.as[QzMemberPaperQuestion].map(ele=>{
      ele.score=BigDecimal.apply(ele.score).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
      ele
    }).select("userid","paperviewid","chapterid","sitecourseid","questionid","majorid","useranswer","istrue","lasttime","opertype","paperid","spendtime","score","question_answer","dt","dn"
    )//.write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_member_paper_question")


    qzChapter.select("chapterid","chapterlistid","chaptername","sequence","showstatus","creator","createtime","courseid","chapternum","outchapterid","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter")


    qzChapterList.withColumn("sequence",lit("-")).select("chapterlistid","chapterlistname","courseid","chapterallnum","sequence","status","creator","createtime","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter_list")


    qzPointQuestion.select("pointid","questionid","questype","creator","createtime","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point_question")


    qzSiteCourse.select("sitecourseid","siteid","courseid","sitecoursename","coursechapter","sequence","status","creator","createtime","helpparperstatus","servertype","boardid","showstatus","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_site_course")


    qzCourse.withColumn("coursechapter",lit("-")).select("courseid","majorid","coursename","coursechapter","sequence","isadvc","creator","createtime","status","chapterlistid","pointlistid","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_course")


    qzCourseEduSubject.select("courseeduid","edusubjectid","courseid","creator","createtime","majorid","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_course_edusubject")


    qzWebsite.select("siteid","sitename","domain","sequence","multicastserver","templateserver","status","creator","createtime","multicastgateway","multicastport","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_website")


    qzMajor.select("majorid","businessid","siteid","majorname","shortname","status","sequence","creator","createtime","columm_sitetype","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_major")


    qzBusiness.select("businessid","businessname","sequence","status","creator","createtime","siteid","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_business")


    qzPaperView.withColumn("conteststarttime",lit("-")).withColumn("contestendtime",lit("-")).withColumn("status",lit("-")).select("paperviewid","paperid","paperviewname","paperparam","openstatus","explainurl","iscontest","contesttime","conteststarttime","contestendtime","contesttimelimit","dayiid","status","creator","createtime","paperviewcatid","modifystatus","description","papertype","downurl","paperuse","paperdifficult","testreport","paperuseshow","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_paper_view")


    qzCenterPaper.select("paperviewid","centerid","openstatus","sequence","creator","createtime","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center_paper")

    qzCenter.select("centerid","centername","centeryear","centertype","openstatus","centerparam","description","creator","createtime","sequence","provideuser","centerviewtype","stage","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center")

    qzQuestionType.select("quesviewtype","viewtypename","questypeid","description","status","creator","createtime","papertypename","sequence","remark","splitscoretype","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_question_type")
  }
}
