package com.zy.chapter06

import com.zy.chapter05.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @create 2020-01-26
  * @author zhouyu
  * @desc 周期性水位线,该分配器会返回一个时间戳等于最大时间戳减去1分钟容忍间隔的水位线
  */
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{
  val bound : Long = 60 * 1000
  var maxTs : Long = Long.MinValue
  override def getCurrentWatermark: Watermark = {
    //generate one minute watermark
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.compareTo(element.timestamp)
    element.timestamp
  }
}
