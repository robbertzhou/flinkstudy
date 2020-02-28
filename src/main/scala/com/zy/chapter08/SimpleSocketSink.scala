package com.zy.chapter08

import java.io.PrintStream
import java.net.{InetAddress, Socket}

import com.zy.chapter05.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * create 2020-02-28
 * author zhouyu
 * desc 简单的socket flink sink函数
 */
class SimpleSocketSink extends RichSinkFunction[SensorReading]{
  var socket : Socket = _
  var writer : PrintStream = _

  override def open(parameters: Configuration): Unit = {
    socket = new Socket(InetAddress.getByName("myslave"),9191)
    writer = new PrintStream(socket.getOutputStream)
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    writer.println(value.toString)
    writer.flush()
  }

  override def close(): Unit = {
    writer.close()
    socket.close()
  }
}
