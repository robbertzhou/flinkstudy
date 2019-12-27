package com.zy.datastreamapi

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * 从集合中创建数据源
  */
object CollectionSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val src = env.fromElements(Tuple2(2,"jj"),Tuple2(3,"aa"),Tuple2(4,"bb"))
    src.print()
    env.execute("collect")
  }
}
