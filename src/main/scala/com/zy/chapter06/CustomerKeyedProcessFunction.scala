package com.zy.chapter06

import com.zy.chapter05.SensorReading
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * @create 2020-01-27
  * @author zhouyu
  * @desc keybyliu后的处理函数
  */
class CustomerKeyedProcessFunction extends KeyedProcessFunction[String,SensorReading,SensorReading]{
  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#OnTimerContext,
                       out: Collector[SensorReading]): Unit = {

  }
}
