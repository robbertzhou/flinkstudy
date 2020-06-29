package com.zy.fbmproj

import java.lang

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction

//import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction

//import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MyWindow extends ProcessAllWindowFunction[TortEntity,String,TimeWindow]{
  override def process(context: Context, elements: Iterable[TortEntity], out: Collector[String]): Unit = {

  }
}
