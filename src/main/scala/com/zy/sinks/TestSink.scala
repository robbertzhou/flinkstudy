package com.zy.sinks

import com.zy.commonapi.NoParallelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @create 2020-01-17
  * @author zhouyu
  * @desc: 测试自定义的sink
  */
object TestSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new NoParallelSource)
    source.addSink(new MySink())
    source.addSink(new SecondSink())
    env.execute("")
  }
}
