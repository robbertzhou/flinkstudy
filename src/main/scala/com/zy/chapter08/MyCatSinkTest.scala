package com.zy.chapter08

import com.zy.chapter05.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * create 2020-02-28
 * author zhouyu
 * desc 测试mycat
 */
object MyCatSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.addSource(new SensorSource)
    src.addSink(new MycatUpsertSink)
    env.execute("mycat upsert job")
  }
}
