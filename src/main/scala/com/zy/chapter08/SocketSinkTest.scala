package com.zy.chapter08

import com.zy.chapter05.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * create 2020-02-28
 * author zhouyu
 * desc socket sink的启动函数
 */
object SocketSinkTest {
  def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment
   val src = env.addSource[SensorReading](new SensorSource)
    src.addSink(new SimpleSocketSink)
    env.execute("customer socket sink")
  }
}
