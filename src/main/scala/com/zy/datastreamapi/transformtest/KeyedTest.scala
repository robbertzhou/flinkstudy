package com.zy.datastreamapi.transformtest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * keyby练习
  */
object KeyedTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.fromElements((1,3),(1,4),(3,5),(3,1),(5,1))
    val keyed = src.keyBy(0)
    keyed.print()  //只是执行了分区操作
    val reduced = keyed.reduce((t1,t2)=>(t1._1,t1._2+t2._2))
    reduced.print()
    env.execute("job")
  }
}
