package com.zy.chapter05

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @create 2020-01-26
  * @author zhouyu
  * @desc 练习流的Union
  */
object UnionStreamTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parisStream = env.addSource(new SensorSource)
    val tokyoStream = env.addSource(new SensorSource)
    val rioStream = env.addSource(new SensorSource)
    val allCities = parisStream.union(tokyoStream,rioStream)
    allCities.print().setParallelism(2)
    env.execute("union stream")
  }
}
