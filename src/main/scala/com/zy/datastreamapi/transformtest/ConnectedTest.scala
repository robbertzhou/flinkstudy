package com.zy.datastreamapi.transformtest

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * 两个ds进行连接(两个类型可以不一致)
  * connectedstream不能进行print
  * connectedstream的map操作要用两个map函数
  * 统一类型结果
  */
object ConnectedTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream1 = env.fromElements(("a",1),("b",2),("c",3),("d",4),("e",5))
    val dataStream2 = env.fromElements(1,2,3,4,5)
    val connectedStream:ConnectedStreams[(String,Int),Int] = dataStream1.connect(dataStream2)
    val maped = connectedStream.map(new CoMapFunction[(String,Int),Int,(Int,String)] {
      override def map1(in1: (String, Int)): (Int, String) = {
        (in1._2,in1._1)
      }

      override def map2(in2: Int): (Int, String) = {
        (in2,"Default")
      }
    })
    maped.print()
    env.execute("connected")
  }
}
