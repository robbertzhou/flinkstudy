package com.zy.labelcompute

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 解析病结构化数据
 */
object LabelFactorCompute {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream("localhost",8090,'\n')
    val maped = source.map(new MapFunction[String,String] {
      override def map(value: String): String = {
        "sss"
      }
    })
    maped.print()
    env.execute("labelCompute")
  }
}
