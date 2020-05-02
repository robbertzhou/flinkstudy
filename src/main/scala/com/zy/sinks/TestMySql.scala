package com.zy.sinks

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/***
 *
 * @create 2020-01-17
  * @author zhouyu
  * @desc 测试mysql sink
  */
object TestMySql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.addSource(new RichSourceFunction[Row] {
      override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit = {
        while(true){
          val rand = new Random()
          val id = rand.nextInt(300000) + ""
          val name=id +"jjjj"
          val row = new Row(2)
          row.setField(0,id)
          row.setField(1,name)
          sourceContext.collect(row)
        }
      }

      override def cancel(): Unit = {}
    })

    val frm = JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername("com.mysql.jdbc.Driver").setDBUrl("jdbc:mysql://192.168.0.126:3306/testdb")
      .setUsername("root")
      .setPassword("mima")
      .setQuery("insert into t_test(id,name) values(?,?)")
      .finish()
    src.writeUsingOutputFormat(frm)
    env.execute("hbasesink")
  }
}
