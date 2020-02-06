package com.zy.sinks

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @create 2020-01-17
  * @author zhouyu
  * @desc 测试postgresql的sink
  */
object TestPostgresqlSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new StudentSource())
    source.addSink(new PostgresqlSink)
    env.execute("lll")
  }
}
