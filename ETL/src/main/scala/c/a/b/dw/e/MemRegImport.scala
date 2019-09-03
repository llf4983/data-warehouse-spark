package c.a.b.dw.e

import c.a.b.dw.bean.UserRegType
import org.apache.spark.sql.SparkSession

object MemRegImport {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("memreg").master("local[*]").enableHiveSupport().getOrCreate()

    ss.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set hive.exec.compress.output=true")


    val dataFrame = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/memberRegtype.log")
    import ss.implicits._
    val dataSet = dataFrame.as[UserRegType]
    val addDS = dataSet.map(ele => {
      if ("1".equals(ele.regsource)) {
        (ele.uid,ele.appkey,ele.appregurl,ele.bdp_uuid,ele.createtime,ele.domain,ele.isranreg,ele.regsource,"PC",ele.websiteid,ele.dt,ele.dn)
      } else if ("2".equals(ele.regsource)) {
        (ele.uid,ele.appkey,ele.appregurl,ele.bdp_uuid,ele.createtime,ele.domain,ele.isranreg,ele.regsource,"MOBILE",ele.websiteid,ele.dt,ele.dn)
      } else if ("3".equals(ele.regsource)) {
        (ele.uid,ele.appkey,ele.appregurl,ele.bdp_uuid,ele.createtime,ele.domain,ele.isranreg,ele.regsource,"APP",ele.websiteid,ele.dt,ele.dn)
      } else if ("4".equals(ele.regsource)) {
        (ele.uid,ele.appkey,ele.appregurl,ele.bdp_uuid,ele.createtime,ele.domain,ele.isranreg,ele.regsource,"WECHAT",ele.websiteid,ele.dt,ele.dn)
      } else {
        (ele.uid,ele.appkey,ele.appregurl,ele.bdp_uuid,ele.createtime,ele.domain,ele.isranreg,ele.regsource,"other",ele.websiteid,ele.dt,ele.dn)
      }

    })
    addDS.write.mode("overwrite").insertInto("dwd.dwd_member_regtype")

    ss.stop()
  }

}
