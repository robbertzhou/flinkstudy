package com.zy.sql

class Order (ll:Long,pp:String,aa:Int){
  var user:Long = ll
  var product:String = pp
  var amount : Int = aa

  override def toString: String = {
    "用户名：" + user + ",产品：" + product + ",销售金额：" + amount
  }
}
