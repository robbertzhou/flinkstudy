package com.zy.deepinflink.chapter05

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.log4j._

/**
 * @author zy
 * @date 2020-12-05
 */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost",9999)
      .flatMap(new Splitter)
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    stream.print()
    env.execute("windowWordCount")
  }
}

class Splitter extends FlatMapFunction[String,Tuple2[String,Integer]]{
  override def flatMap(sentence: String, collector: Collector[(String, Integer)]): Unit = {
    val splited = sentence.split(" ")
    for(i <- 0 to splited.length - 1){
      collector.collect((splited(i),1))
    }
  }
}
