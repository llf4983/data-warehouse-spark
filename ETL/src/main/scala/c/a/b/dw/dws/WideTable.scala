package c.a.b.dw.dws

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object WideTable {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf = new SparkConf().setAppName("widetable").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    var time="20190722"

    ss.sql("set hive.exec.dynamic.partition.mode=nostrict")
    ss.sql("set mapred.output.compress=true")
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")

    //    ss.sql(s"select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin),first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode),first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain),first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename),first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level),min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level),first(vip_operator),dt,dn from(select a.uid,a.ad_id,\na.fullname,\na.iconurl,\na.lastlogin,\na.mailaddr,\na.memberlevel,\na.password,\ne.paymoney,\na.phone,\na.qq,\na.register,\na.regupdatetime,\na.unitname,\na.userip,\na.zipcode,\na.dt,\n\nb.appkey,\nb.appregurl,\nb.bdp_uuid,\nb.createtime as reg_createtime,\nb.domain,b.isranreg,b.regsource,\nb.regsourcename,\n\nc.adname,\n\nd.siteid,\nd.sitename,\nd.siteurl,\nd.delete as site_delete,\nd.createtime as site_createtime,\nd.creator as site_creator,\n\nf.vip_id,\nf.vip_level,\nf.start_time as vip_start_time,\nf.end_time as vip_end_time,\nf.last_modify_time as vip_last_modify_time,\nf.max_free as vip_max_free,\nf.min_free as vip_min_free,\nf.next_level as vip_next_level,\nf.operator as vip_operator,a.dn \n\nfrom dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid and a.dn=b.dn \nleft join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn \nleft join dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn \nleft join dwd.dwd_pcentermempaymoney e on a.uid=e.uid and a.dn=e.dn \nleft join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn \nwhere a.dt='${time}')\n\n\n group by uid,dn,dt").write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")
    ss.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
      "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
      "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
      "first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain)," +
      "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
      "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
      "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
      "first(vip_operator),dt,dn from" +
      "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
      "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
      "a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource," +
      "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
      "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
      "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
      "f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
      s"from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
      "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
      " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
      s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${time}')r  " +
      "group by uid,dn,dt").write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")
  }
}
