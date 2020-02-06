package com.zy.chapter07

import com.zy.chapter05.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector


/**
  * create 2020-01-29
  * author zhouyu
  * desc 温度警报函数，测试Flink的valuestate状态控制
  */
class TemperatureAlertFunction(val thresold:Double)
  extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  //状态对象引用
  private var lastTempState : ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //create state descriptor
    val descriptor = new ValueStateDescriptor[Double]("lastTemp",classOf[Double])
    //get state reference
    lastTempState = getRuntimeContext().getState[Double](descriptor)
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //从状态中获取上一次的温度
    val lastTemp = lastTempState.value()
    val tempDiff = (value.temperature - lastTemp).abs
    if(tempDiff > thresold){
      out.collect((value.id,value.timestamp,value.temperature))
    }
    this.lastTempState.update(value.temperature)
  }
}
