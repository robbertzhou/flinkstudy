package com.zy.chapter05




import java.lang

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector

/**
  * @create 2020-01-23
  * @author zhouyu
  * @desc 测试传感源
  */
object AverageSensoReadings {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //必需设置watermark的interval。否则报 no water mark
    env.getConfig.setAutoWatermarkInterval(1000L)
    val source = env.addSource(new SensorSource())
//    source.assignAscendingTimestamps()
    val rk = source.assignTimestampsAndWatermarks(new STA())

    val maped = rk.map(sensor =>{
      val celsi = (sensor.temperature - 32) * (5.0/9.0)
      new SensorReading(sensor.id,sensor.timestamp,celsi.toDouble)
    })
    val windowed : DataStream[SensorReading] = maped.keyBy(_.id)
        .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
        .apply(new TAA)
//        .apply(new TemperatureAvgWindow())
    windowed.print()
    env.execute("sensor data reader.")

  }
}

class STA
  extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {

  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: SensorReading): Long = r.timestamp

}

/** User-defined WindowFunction to compute the average temperature of SensorReadings */
class TAA extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
                      sensorId: String,
                      window: TimeWindow,
                      vals: Iterable[SensorReading],
                      out: Collector[SensorReading]): Unit = {

    // compute the average temperature
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    // emit a SensorReading with the average temperature
    out.collect(new SensorReading(sensorId, window.getEnd, avgTemp))
  }
}
