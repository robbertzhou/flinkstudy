package com.zy.flinkshizhan.chapter04

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class MyRichMapfunction extends RichMapFunction[Int,String]{
  override def open(parameters: Configuration): Unit = {
    println(parameters.getString("mykey","kke"))
  }
  override def map(in: Int): String = {
    in + "out"
  }
}
