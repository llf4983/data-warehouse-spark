package c.a.b.dw.e

import c.a.b.dw.bean.BaseWeb
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object BaseWebImport {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf = new SparkConf().setMaster("local[*]").setAppName("baseweb")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val dataFrame = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/baswewebsite.log")

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")

//    ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/baswewebsite.log")
//    dataFrame.write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
//    dataFrame.show()
    import ss.implicits._
    val dataSet = dataFrame.as[BaseWeb]
    val tuple = dataSet.map(ele => {
      (ele.siteid, ele.sitename, ele.siteurl, ele.delete, ele.createtime, ele.creator, ele.dn)
    })
    tuple.write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
//    dataSet.show()
  }

}
