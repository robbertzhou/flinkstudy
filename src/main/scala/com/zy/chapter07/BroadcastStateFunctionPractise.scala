package com.zy.chapter07

import com.zy.chapter05.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * create 2020-01-30
  * author zhouyu
  * desc 广播状态函数
  */
object BroadcastStateFunctionPractise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source : DataStream[SensorReading] = env.addSource(new SensorSource)
    val broadData : DataStream[ThresholdUpdate]= env.fromElements(
      new ThresholdUpdate("sensor_1",3.0d),
      new ThresholdUpdate("sensor_2",2.0d),
      new ThresholdUpdate("sensor_3",11.0d),
      new ThresholdUpdate("sensor_4",15.0d),
      new ThresholdUpdate("sensor_5",12.0d)
    )
    val keyedStream : KeyedStream[SensorReading,String] =
      source.keyBy(_.id)

    val broadcastStateDescriptor =
      new MapStateDescriptor[String,Double]("thresholds",classOf[String],classOf[Double])

    val broadcastThresholds : BroadcastStream[ThresholdUpdate] =
      broadData.broadcast(broadcastStateDescriptor)

    val alerts = keyedStream.connect(broadcastThresholds)
      .process(new UpdateTemperatureAlertFunction)
    alerts.print()
    env.execute("update value")
  }
}
