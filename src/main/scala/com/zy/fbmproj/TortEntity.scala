package com.zy.fbmproj
//unique_key varchar with(primary_key=true),
//dep_id varchar,
//dep_name varchar,
//dim_date varchar
case class TortEntity(uniqueKey:String,depId:String,depName:String,dimDate:String) extends Serializable
