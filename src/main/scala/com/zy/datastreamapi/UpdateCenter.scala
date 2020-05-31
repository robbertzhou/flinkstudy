package com.zy.datastreamapi

import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, CoProcessFunction}
import org.apache.flink.util.Collector

class UpdateCenter extends BroadcastProcessFunction[String,(Long,String),String]{
  override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, (Long, String), String]#ReadOnlyContext, collector: Collector[String]): Unit = {
    collector.collect("jackkk")
  }

  override def processBroadcastElement(in2: (Long, String), context: BroadcastProcessFunction[String, (Long, String), String]#Context, collector: Collector[String]): Unit = {

  }
}
