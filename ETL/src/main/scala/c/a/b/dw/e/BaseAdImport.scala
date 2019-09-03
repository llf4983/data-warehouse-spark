package c.a.b.dw.e

import org.apache.spark.sql.{SaveMode, SparkSession}

object BaseAdImport {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val ss = SparkSession.builder().appName("baselog").master("local[*]").enableHiveSupport().getOrCreate()
    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")

    val dataFrame = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/baseadlog.log")
    import ss.implicits._
    val tuple = dataFrame.map(ele => {
      (ele.getString(0), ele.getString(1), ele.getString(2))

    })
    tuple.write.mode(SaveMode.Append).insertInto("dwd.dwd_base_ad")

  }
}
