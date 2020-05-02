package com.zy.work

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


/**
 * create 2020-05-02
 * author zy
 *
 */
object MaxwellKafkaToTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /**
     * 配置kafka source
     */
    val topic = "maxwell"
    val props = new Properties
    props.setProperty("bootstrap.servers", "slave1.zy.com:9092")
    props.setProperty("max.poll.records","100")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("group.id", "ss11")

    val myConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, props)

    //        offsets.put(new KafkaTopicPartition("topic_name", 1), 22L)
    //        offsets.put(new KafkaTopicPartition("topic_name", 2), 33L)
    //    myConsumer.setStartFromSpecificOffsets(offsets)
    /**
     * 获取kafka数据
     * 格式：//{"dt":"审核时间[年月日 时分秒']","type":"审核类型","username":"审核人姓名","area":"大区"}
     */
      myConsumer.setStartFromEarliest()
    val data = env.addSource(myConsumer)
    data.map(str =>{
      val jsonObj = JSON.parseObject(str)
      jsonObj.remove("database")
      jsonObj.remove("ts")
      jsonObj.remove("xoffset")
      val tableName = jsonObj.get("table").toString
      //发送对应表的数据到对应的topic
      sendMsg(tableName,jsonObj.toString)
      ""
    }).print()
    env.execute("mysql to kudu")
  }

  val props = new Properties
  props.put("bootstrap.servers", "master.zy.com:9092")
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)
  //指定topic名称
  val topic = "auditLog"
  //create kafka connection
  val producer = new KafkaProducer[String, String](props)

  def sendMsg(topic:String,message:String): Unit ={
    producer.send(new ProducerRecord[String, String](topic, message))
  }
}
