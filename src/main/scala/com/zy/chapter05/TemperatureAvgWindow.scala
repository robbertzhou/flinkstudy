package com.zy.chapter05

import java.lang

import org.apache.flink.streaming.api.windowing.windows.TimeWindow

//import org.apache.flink.streaming.api.functions.windowing.WindowFungetWindowStartWithOffsetction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector

/**
  * @create 2020-01-24
  * @author zhouyu
  * @desc 窗口函数,
  * @错误总结 WindowFunction使用不对
  */
class TemperatureAvgWindow extends WindowFunction[SensorReading ,SensorReading,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[SensorReading],
                     out: Collector[SensorReading]): Unit = {
    var cnt = 0.0
    var sum = 0.0
    val it = input.iterator
    while (it.hasNext){
      val vv = it.next()
      sum += vv.temperature
      cnt += 1
    }
    out.collect(new SensorReading(key,window.getEnd,sum / cnt))
  }
}
