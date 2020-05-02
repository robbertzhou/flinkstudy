package com.zy.work

import java.util
import java.util.Properties

import org.apache.commons.collections.map.HashedMap
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
 * create 2020-05-01
 * zhouyu
 */
object MysqlToKudu {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /**
     * 配置kafka source
     */
    val topic = "test2"
    val props = new Properties
    props.setProperty("bootstrap.servers", "master.zy.com:9092")
    props.setProperty("max.poll.records","100")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("group.id", "ss")

    val myConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, props)
        import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
        val offsets = new util.HashMap[KafkaTopicPartition,java.lang.Long]()
    val top = new KafkaTopicPartition("",0)
        offsets.put(top,2790790L)
//        offsets.put(new KafkaTopicPartition("topic_name", 1), 22L)
//        offsets.put(new KafkaTopicPartition("topic_name", 2), 33L)
//    myConsumer.setStartFromSpecificOffsets(offsets)
    /**
     * 获取kafka数据
     * 格式：//{"dt":"审核时间[年月日 时分秒']","type":"审核类型","username":"审核人姓名","area":"大区"}
     */
    val data = env.addSource(myConsumer)
    data.print()
    env.execute("mysql to kudu")
  }
}
