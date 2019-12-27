package com.zy.datastreamapi

import java.util

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api._
/**
  * 从数组转换为数据源
  */
object FromArray {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val names = new Array[String](3)
    names(0) = "Jack"
    names(1) = "Mike"
    names(2) = "Jenny"
    val list = names.toList.toIterator.toSeq
    val src = env.fromElements(list)
    src.print()
    env.execute("array")
  }
}
