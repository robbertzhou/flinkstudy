package com.zy.chapter06

import com.zy.chapter05.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
/**
  * @create 2020-01-27
  * @author zhouyu
  * @desc 温度警报处理函数
  */
class TempIncreaseAlertFunction extends KeyedProcessFunction[String,SensorReading,String]{
  lazy val lastTemp : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]
  ("lastTemp",Types.of[Double]))

  lazy val currentTime : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double](
    "timer",Types.of[Double])
  )
  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    //获取前一个温度
    val prevTemp = lastTemp.value()
    //更新最近一次的温度
    lastTemp.update(value.temperature)
    val curTimerTimestamp = currentTime.value()

  }
}
