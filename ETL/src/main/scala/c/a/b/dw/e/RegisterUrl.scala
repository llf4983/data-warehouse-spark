package c.a.b.dw.e

import c.a.b.dw.bean.RegisterUrlLog
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object RegisterUrl {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf = new SparkConf().setAppName("urlcount").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
//    ss.sql("set hive.exec.compress.output=true")
//    ss.sql("set mapred.output.compress=true")
//    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
//    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
//    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    import ss.implicits._
    val regUrl = ss.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname,siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ").as[RegisterUrlLog]
    regUrl.cache()


    regUrl.mapPartitions(par=>{
      par.map(ele => {(ele.appregurl + "_" + ele.dt + "_" + ele.dn, 1)})
    }).groupByKey(ele => ele._1).mapValues(ele => {
      ele._2
    }).reduceGroups((ele1,ele2)=>ele1+ele2).map(ele=>{
      val strings = ele._1.split("_")
      (strings(0),ele._2,strings(1),strings(2))
    }).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")



    regUrl.mapPartitions(par=>{
      par.map(ele => {(ele.sitename + "_" + ele.dt + "_" + ele.dn, 1)})
    }).groupByKey(ele => ele._1).mapValues(ele => {
      ele._2
    }).reduceGroups((ele1,ele2)=>ele1+ele2).map(ele=>{
      val strings = ele._1.split("_")
      (strings(0),ele._2,strings(1),strings(2))
    }).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_sitenamenum")



    regUrl.mapPartitions(par=>{
      par.map(ele => {(ele.regsourcename + "_" + ele.dt + "_" + ele.dn, 1)})
    }).groupByKey(ele => ele._1).mapValues(ele => {
      ele._2
    }).reduceGroups((ele1,ele2)=>ele1+ele2).map(ele=>{
      val strings = ele._1.split("_")
      (strings(0),ele._2,strings(1),strings(2))
    }).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_regsourcenamenum")


    regUrl.mapPartitions(par=>{
      par.map(ele => {(ele.adname + "_" + ele.dt + "_" + ele.dn, 1)})
    }).groupByKey(ele => ele._1).mapValues(ele => {
      ele._2
    }).reduceGroups((ele1,ele2)=>ele1+ele2).map(ele=>{
      val strings = ele._1.split("_")
      (strings(0),ele._2,strings(1),strings(2))
    }).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_adnamenum")

    regUrl.mapPartitions(par=>{
      par.map(ele => {(ele.memberlevel + "_" + ele.dt + "_" + ele.dn, 1)})
    }).groupByKey(ele => ele._1).mapValues(ele => {
      ele._2
    }).reduceGroups((ele1,ele2)=>ele1+ele2).map(ele=>{
      val strings = ele._1.split("_")
      (strings(0),ele._2,strings(1),strings(2))
    }).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_memberlevelnum")


    regUrl.mapPartitions(par=>{
      par.map(ele => {(ele.vip_level+ "_" + ele.dt + "_" + ele.dn, 1)})
    }).groupByKey(ele => ele._1).mapValues(ele => {
      ele._2
    }).reduceGroups((ele1,ele2)=>ele1+ele2).map(ele=>{
      val strings = ele._1.split("_")
      (strings(0),ele._2,strings(1),strings(2))
    }).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_viplevelnum")


import org.apache.spark.sql.functions._
    regUrl.withColumn("rownumber",row_number().over(Window.partitionBy("dn","memberlevel").orderBy(desc("paymoney"))))
      .where("rownumber<4").orderBy("memberlevel","rownumber")//.show()//
      .select("uid","memberlevel","register","appregurl","regsourcename","adname","sitename","vip_level","paymoney","rownumber","dt","dn").write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")
  }
}
