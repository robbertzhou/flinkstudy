package com.zy.sinks

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction


/**
  * @create 2020-01-22
  * @author zhouyu
  * @desc 测试两个sink的使用
  */
class SecondSink extends RichSinkFunction[Long]{
  override def invoke(ll:Long): Unit = {
    println("second is :" + ll)
  }
}
