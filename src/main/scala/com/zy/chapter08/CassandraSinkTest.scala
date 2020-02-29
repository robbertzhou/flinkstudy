package com.zy.chapter08

import com.zy.chapter05.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.api.scala._

/**
 * create 2020-02-29
 * author zhouyu
 * desc cassandra data sink练习
 */
object CassandraSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.addSource[SensorReading](new SensorSource)
    val maped = src.map(sen =>{
      (sen.id,sen.temperature.toDouble)
    })
    val sinkBuilder = CassandraSink.addSink(maped)
    sinkBuilder.setHost("slave2",9042)
      .setQuery("insert into mydb.sensors(sensorid,temperature) values(?,?)")
      .build()
    //停顿几秒才能创建好builder，在写数据时，查询会超时的现象。
    Thread.sleep(1000)
    env.execute("cassandra test")
  }
}
