package c.a.b.dw.e

import c.a.b.dw.bean.VipLevel
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object VipLogImport {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf = new SparkConf().setAppName("viplog").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")

    val dataFrame = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/pcenterMemViplevel.log")
import ss.implicits._
    val dataSet = dataFrame.as[VipLevel]

    val tuple = dataSet.map(ele => {
      (ele.vip_id, ele.vip_level, ele.start_time, ele.end_time, ele.last_modify_time, ele.max_free, ele.min_free, ele.next_level, ele.operator, ele.dn)
    })
    tuple.write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
  }
}
