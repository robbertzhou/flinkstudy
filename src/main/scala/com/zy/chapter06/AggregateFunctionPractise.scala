package com.zy.chapter06

import com.zy.chapter05.{SensorSource, TemperatureAvgWindow}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
  * @create 2020-01-28
  * @author zhouyu
  * @desc 聚合函数练习
  */
object AggregateFunctionPractise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new SensorSource)
    val rd = source.map(r => (r.id,r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .aggregate(new AvgTempFunction)
    rd.print()
    env.execute("aggregate function")
  }
}
