package com.zy.chapter06

import com.zy.chapter05.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @create 2020-01-28
  * @author zhouyu
  * @desc reduce函数处理
  */
object ReducePractise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new SensorSource)
    val rd = source.map(r => (r.id,r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((r1,r2) => (r1._1,r1._2.min(r2._2)))
    rd.print()
    env.execute("reduce function")
  }
}
