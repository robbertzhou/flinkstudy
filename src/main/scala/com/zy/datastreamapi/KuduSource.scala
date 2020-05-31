package com.zy.datastreamapi

import java.sql.DriverManager

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.api.scala._
/**
 * create 2020-05-31
 * author zhouyu
 * desc
 */
class KuduSource extends RichSourceFunction[(Long,String)]{
  Class.forName("com.cloudera.impala.jdbc41.Driver")

  override def open(parameters: Configuration): Unit = {

  }
  override def run(sourceContext: SourceFunction.SourceContext[(Long,String)]): Unit = {
    val conn = DriverManager.getConnection("jdbc:impala://m.zy.com:21050/test")
    val ps = conn.prepareStatement("select * from deps")
    val rs = ps.executeQuery
    while (rs.next()){
      val id = rs.getLong(1)
      val name = rs.getString(2)
      sourceContext.collect((id,name))
    }
  }

  override def cancel(): Unit = {

  }
}
