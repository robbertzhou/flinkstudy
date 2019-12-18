package com.zy.commonapi

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object TestSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //没有并行度的
    val src = env.addSource(new MyParallelSource)
    src.setParallelism(3) //自定义并行度
    src.print()
    env.execute("job")
  }
}
