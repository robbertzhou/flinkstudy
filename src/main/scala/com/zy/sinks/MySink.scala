package com.zy.sinks

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * @create 2020-01-17
  * @author zhouyu
  * @desc:自定义一个sink，输入数据类型为Long。
  */
class MySink extends RichSinkFunction[Long]{
  override def open(parameters: Configuration): Unit = {

  }

  override def invoke(value: Long): Unit = {
    println("test value is : " + value)
  }

  override def close(): Unit = {

  }
}
