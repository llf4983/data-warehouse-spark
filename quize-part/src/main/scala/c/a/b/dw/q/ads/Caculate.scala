package c.a.b.dw.q.ads

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window

object Caculate {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    val conf = new SparkConf().setAppName("adscal").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
//    ss.sql("set mapred.output.compress=true")
//    ss.sql("set hive.exec.compress.output=true")
//    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
//    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
//    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")


    val date=20190722

    val scoreOper = ss.sql("select paperviewid,paperviewname,score,spendtime,dt,dn from dws.dws_user_paper_detail").where(s"dt=$date")
import org.apache.spark.sql.functions._


    scoreOper.groupBy("paperviewid","paperviewname","dt","dn").agg(avg("score").cast("decimal(4,1)").as("avgscore"),
      avg("spendtime").cast("decimal(10,1)").as("avgspendtime")
    ).select("paperviewid","paperviewname","avgscore","avgspendtime","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_avgtimeandscore")

    scoreOper.groupBy("paperviewid","paperviewname","dt","dn").agg(max("score").as("maxscore"),
      min("score").as("minscore")
    ).select("paperviewid","paperviewname","maxscore","minscore","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_maxdetail")


    import ss.implicits._
    val dataFrame = ss.sql(s"select * from dws.dws_user_paper_detail where dt=$date")
    dataFrame.cache()

    dataFrame.select("userid","paperviewid","paperviewname","chaptername","pointname","sitecoursename","coursename","majorname","shortname","papername","score","dt","dn"
    ).withColumn("rank",dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
      .where("rank<4").select("userid","paperviewid","paperviewname","chaptername","pointname","sitecoursename","coursename","majorname","shortname","papername","score","rank","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("ads.ads_top3_userdetail")



    dataFrame.select("userid","paperviewid","paperviewname","chaptername","pointname","sitecoursename","coursename","majorname","shortname","papername","score","dt","dn"
    ).withColumn("rank",dense_rank().over(Window.partitionBy("paperviewid").orderBy("score")))
      .where("rank<4").select("userid","paperviewid","paperviewname","chaptername","pointname","sitecoursename","coursename","majorname","shortname","papername","score","rank","dt","dn"
    ).write.mode(SaveMode.Overwrite).insertInto("ads.ads_low3_userdetail")

//    dataFrame.select("userid","paperviewid","paperviewname","chaptername","pointname","sitecoursename","coursename","majorname","shortname","papername","score","dt","dn"
//    ).withColumn("rank",dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
//      .where("rank<4").select()

    dataFrame.select("paperviewid","paperviewname","score","userid","dt","dn").withColumn(
      "seg",when(col("score").between(0,20),"0-20")
        .when(col("score")>20&&col("score")<=40,"20-40")
        .when(col("score")>40&&col("score")<=60,"40-60")
        .when(col("score")>60&&col("score")<=80,"60-80")
        .when(col("score")>80&&col("score")<=100,"80-100")
    ).drop("score").groupBy("paperviewid","paperviewname","seg","dt","dn")
      .agg(concat_ws(",",collect_list(col("userid").cast("string").as("userids"))).as("userids"))
      .select("paperviewid","paperviewname","seg","userids","dt","dn")
      .orderBy("paperviewid","seg"
      ).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_scoresegment_user")

    val unpass = dataFrame.select("paperviewid", "paperviewname", "dt", "dn").where("score between 0 and 60").groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(count("paperviewid").as("unpass"))

//    unpass.show()
    val pass = dataFrame.select("paperviewid", "dn").where("score >=60").groupBy("paperviewid",   "dn")
      .agg(count("paperviewid").as("pass"))
//    pass.show()

    unpass.join(pass,Seq("paperviewid","dn")).withColumn("rate",(col("pass")/(col("pass")+col("unpass"))).cast("decimal(4,2)")
    ).select("paperviewid","paperviewname","unpass","pass","rate","dt","dn")
      .write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_paper_detail")//.show()


    val correct = dataFrame.select("questionid", "dt", "dn").where("user_question_answer='1'").groupBy("questionid", "dn", "dt")
      .agg(count("questionid").as("correct"))


    val uncorrect = dataFrame.select("questionid", "dn").where("user_question_answer='0'").groupBy("questionid", "dn")
      .agg(count("questionid").as("uncorrect"))
    correct.join(uncorrect,Seq("questionid","dn")).withColumn("rate", (col("correct")/(col("correct")+col("uncorrect"))).cast("decimal(4,2)"))
      .select("questionid","uncorrect","correct","rate","dt","dn")
      .write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_question_detail")

  }
}
