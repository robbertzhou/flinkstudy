package com.zy.chapter07

import com.zy.chapter05.SensorReading
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

/**
  * create 2020-01-30
  * author zhouyu
  * desc 温度状态函数
  */
class UpdateTemperatureAlertFunction
  extends KeyedBroadcastProcessFunction[String,SensorReading,ThresholdUpdate,(String,Double,Double)]{
  //广播状态的描述符
  private lazy val thresholdStateDescriptor =
    new MapStateDescriptor[String,Double]("thresholds",classOf[String],classOf[Double])
  //键值分区状态值
  private var lastTempState : ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp",classOf[Double])
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }

  override def processElement(value: SensorReading,
                              ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate,
                                (String, Double, Double)]#ReadOnlyContext,
                              out: Collector[(String, Double, Double)]): Unit = {
    val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)
    if(thresholds.contains(value.id)){
      val sensorThreshold = thresholds.get(value.id)
      val lastTemp = lastTempState.value()
      val tempDiff = (value.temperature - lastTemp).abs
      if(tempDiff > sensorThreshold){
        out.collect(value.id,value.temperature,tempDiff)
      }
    }
    this.lastTempState.update(value.temperature)
  }

  override def processBroadcastElement(value: ThresholdUpdate,
                                       ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#Context,
                                       out: Collector[(String, Double, Double)]): Unit = {
    val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)
    if(value.threshold != 0.0d){
      thresholds.put(value.id,value.threshold)
    }else{
      thresholds.remove(value.id)
    }
  }
}
