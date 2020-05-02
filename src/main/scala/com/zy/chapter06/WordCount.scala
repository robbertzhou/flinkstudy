package com.zy.chapter06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * create 2020-04-05
 * author zhouyu
 * desc 统计单词
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val src = env.fromElements("What's ther","hello world")
    val maped = src.flatMap(ele => ele.toLowerCase().split("\\W+"))

    val transed = maped.map(ele => (ele,1))
    val counts = transed.groupBy(0).sum(1)
    counts.print()
//    env.execute("ff")
  }
}
