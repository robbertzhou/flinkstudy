package com.zy.chapter05

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
  * @create 2020-01-23
  * @author zhouyu
  * @desc 数据源（传感器）
  */
import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
  * Flink SourceFunction to generate SensorReadings with random temperature values.
  *
  * Each parallel instance of the source simulates 10 sensors which emit one sensor
  * reading every 100 ms.
  *
  * Note: This is a simple data-generating source function that does not checkpoint its state.
  * In case of a failure, the source does not replay any data.
  */
class SensorSource extends RichParallelSourceFunction[SensorReading] {

  // flag indicating whether source is still running.
  var running: Boolean = true

  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[SensorReading]): Unit = {

    // initialize random number generator
    val rand = new Random()

    // look up index of this parallel task
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    // initialize sensor ids and temperatures
    var curFTemp = (1 to 1000).map {
//      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
//      i => ("sensor_" + (taskIdx * 1 + i), 6 + (rand.nextGaussian() * 2))
      i => ("sensor_" + rand.nextInt(2000000000), 6 + (rand.nextGaussian() * 2))
    }

    // emit data until being canceled
    while (running) {

      // update temperature
      curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      curFTemp.foreach( t => srcCtx.collect(new SensorReading(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(1)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }



}