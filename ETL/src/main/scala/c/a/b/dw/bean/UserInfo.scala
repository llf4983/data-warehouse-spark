package c.a.b.dw.bean

case class UserInfo (
                      var uid:String,
                      var ad_id:String,
                      var birthday:String,
                      var email:String,
                      var fullname:String,
                      var iconurl:String,
                      var lastlogin:String,
                      var mailaddr:String,
                      var memberlevel:String,
                      var password:String,
                      var paymoney:String,
                      var phone:String,
                      var qq:String,
                      var register:String,
                      var regupdatetime:String,
                      var unitname:String,
                      var userip:String,
                      var zipcode:String,
                      var dt:String,
                      var dn:String
                   )extends Serializable{
//  override def toString: String = {
//   uid+","+ad_id+","+birthday+","+email+","+fullname+","+iconurl+","+lastlogin+","+mailaddr+","+
//           memberlevel+","+password+","+paymoney+","+phone+","+qq+","+register+","+regupdatetime+","+unitname+","+userip+","+zipcode
//  }
}
