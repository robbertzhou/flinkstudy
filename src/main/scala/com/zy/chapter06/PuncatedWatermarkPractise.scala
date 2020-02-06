package com.zy.chapter06


import com.zy.chapter05.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * @create 2020-01-27
  * @author zhouyu
  * @desc 测试定点水位线 练习
  */
object PuncatedWatermarkPractise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val source = env.addSource(new SensorSource)
    val assigned = source.assignTimestampsAndWatermarks(new PunctuatedAssigner())
    assigned.print()
    env.execute("puncated watermark")
  }
}
