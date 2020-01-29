package com.zy.chapter07

import com.zy.chapter05.SensorSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * create 2020-01-29
  * author zhouyu
  * desc 练习键值分区状态函数
  */
object KeyStateFunctionPractise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new SensorSource)
    val keyedStream = source.keyBy(_.id)
    val alerts : DataStream[(String,Double,Double)] =
      keyedStream.flatMap(new TemperatureAlertFunction(19))
    alerts.print()
    env.execute("value state practise")
  }
}
