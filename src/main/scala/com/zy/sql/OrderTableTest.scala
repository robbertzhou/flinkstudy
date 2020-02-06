package com.zy.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._

object OrderTableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv : StreamTableEnvironment = new StreamTableEnvironment(env,new TableConfig)
    val srcA = env.addSource(new OrderSource)
    val mm = ""
    tEnv.registerDataStream("Ordera",srcA)
    val tab = tEnv.sqlQuery("select * from Ordera")
//    tEnv.toAppendStream(ta, classOf[Order]).print()
    tEnv.toAppendStream[Order](tab).print()
    env.execute()
  }
}
