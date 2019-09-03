package c.a.b.dw.e


import c.a.b.dw.bean.UserInfo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object ExtractTL {
  def main(args: Array[String]): Unit = {
//    var warehouse=
    val ss = SparkSession.builder().enableHiveSupport().master("local[*]").appName("ETL").getOrCreate()
    val dataFrame = ss.read.json("hdfs://hadoop127:9000/user/atguigu/ods/member.log")
    //    dataFrame.show()

    import ss.implicits._
//    val re = dataFrame.map(ele => {
//      ele.getString(1)
//    })
//    re.show()
    val dataSet = dataFrame.as[UserInfo]
//    dataSet.map(ele=>{
//      ele
//    })
    val cleanDS = dataSet.map(ele => {
      ele.fullname = ele.fullname.charAt(0) + "XX"
      ele.phone=ele.phone.splitAt(3)._1 + "*****" + ele.phone.splitAt(8)._2
      ele.password = "******"
      ele
    })
//    ss.sql("use dwd")
    ss.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    cleanDS.write.mode("append").insertInto("dwd.dwd_member")
//    cleanDS.filter(_.fullname!=null).foreach(ele=>{
//      println(ele.toString)
////      ss.sql("insert into table dwd_member partiton(dt="+ele.dt+","+"dn="+ele.dn+") values("+ele.uid+","+ele.ad_id+","+ele.birthday+","+ele.email+","+ele.fullname+","+ele.iconurl+","+ele.lastlogin+","+ele.mailaddr+","+
////        ele.memberlevel+","+ele.password+","+ele.paymoney+","+ele.phone+","+ele.qq+","+ele.register+","+ele.regupdatetime+","+ele.unitname+","+ele.userip+","+ele.zipcode+");"
////      )
////      ss.sql("insert into table dwd_member partition(dt='"+ele.dt+"',"+"dn='"+ele.dn+"') values("+ele.toString+");")
//    })
//    ss.sql("insert into table dwd_member partition(dt='"+ele.dt+"',"+"dn='"+ele.dn+"') values("+ele.toString+");")
    //    cleanDS.write.save()

    ss.stop()
  }
}
