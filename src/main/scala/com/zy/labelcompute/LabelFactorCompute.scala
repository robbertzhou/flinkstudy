package com.zy.labelcompute

import java.awt

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.function.WindowFunction


/**
 * 解析病结构化数据
 */
object LabelFactorCompute {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream("localhost",8090,'\n')
    val maped = source.map(new MapFunction[String,Tuple2[String,WebLog]] {
      override def map(value: String): Tuple2[String,WebLog] = {
        //eid,time,ip,url,ua,cookieId,command
        val entity = new WebLog
        val splitStr = value.split(",")
        entity.eid = splitStr(0)
        entity.time = splitStr(1)
        entity.ip = splitStr(2)
        entity.url  = splitStr(3)
        entity.cookieID = splitStr(4)
        entity.command = splitStr(5)
        Tuple2(entity.eid,entity)
      }
    })
    val computedLabel = maped.keyBy(1)
    computedLabel.timeWindow(Time.seconds(5))
//    computedLabel.print()
    env.execute("labelCompute")
  }
}
