package com.zy.chapter06

import com.zy.chapter05.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @create 2020-01-27
  * @author zhouyu
  * @desc 定点水位线分配器
  */
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading]{
  var bound = 60 * 1000

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id == "sensor_1"){
      new Watermark(extractedTimestamp - bound)
    }else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp
  }
}
