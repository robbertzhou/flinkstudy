package com.zy.flinkshizhan.chapter04

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * @create 2021-06-27
 * @author zhouyu
 * @desc 处理参数,只能是批处理
 */
object ProcessParameter {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("mykey","myval")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(conf)
    val source = env.fromElements(1,2,4)
    val maped = source.map(new MyRichMapfunction())
    maped.print()
    env.execute("job")
  }
}
