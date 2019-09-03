package c.a.b.dw.dws

import c.a.b.dw.bean.ZipLog
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object ZipTable {
  System.setProperty("HADOOP_USER_NAME","atguigu")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ziptable").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")

    val time=20190722
//    ss.sql("use dws")
    import ss.implicits._
    val dataSet = ss.sql("select * from dws.dws_member_zipper").as[ZipLog]
    val addDate = ss.sql(s"select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level," +
      s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,first(a.dn) as dn " +
      " from dwd.dwd_pcentermempaymoney a join " +
      s"dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn where a.dt='$time' group by uid"
    ).as[ZipLog]
    val unionGroup = addDate.union(dataSet).groupByKey(ele => {
      ele.uid + "_" + ele.dn
    })
    val updateDS = unionGroup.mapGroups((key, ele) => {
      val list = ele.toList.sortWith((ele1, ele2) => ele1.start_time < ele2.start_time)
      if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
        list(list.size - 2).end_time = list(list.size - 1).start_time
        list(list.size - 1).paymoney = BigDecimal.apply(list(list.size - 1).paymoney) + BigDecimal.apply(list(list.size - 2).paymoney).toString()
      }

      list
    })
    updateDS.flatMap(ele=>ele).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")


  }
}
