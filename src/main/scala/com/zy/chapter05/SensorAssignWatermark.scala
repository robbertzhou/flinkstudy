package com.zy.chapter05

import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @create 2020-01-24
  * @author zhouyu
  * @desc 传感器的水印生成器
  */
class SensorAssignWatermark extends AssignerWithPeriodicWatermarks[SensorReading]{
  val bound : Long= 60 * 1000  //1分钟的毫秒数
  var maxTs:Long = Long.MinValue //观察到最大的时间戳
  val current = System.currentTimeMillis()
  override def getCurrentWatermark: Watermark = {
    new Watermark(System.currentTimeMillis)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    if(maxTs <= Long.MinValue){
      maxTs = System.currentTimeMillis()
    }
    maxTs
  }
}
