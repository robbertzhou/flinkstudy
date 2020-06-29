package com.zy.fbmproj

import java.sql.{DriverManager, Timestamp}
import java.util.{Date, Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
 * create 2020-06-27
 * author zhouyu
 * desc 插入kudu
 */
object InsertFbmTortDataVideo {
  def main(args: Array[String]): Unit = {
    Class.forName("com.facebook.presto.jdbc.PrestoDriver");
    val connection = DriverManager.getConnection("jdbc:presto://m.zy.com:8081/kudu/default","hive",null)
    connection.setAutoCommit(false)
    val random = UUID.randomUUID()
//    unique_key varchar with(primary_key=true),
//    dep_id varchar,
//    dep_name varchar,
//    dim_date varchar,
//    ingestion_time timestamp,
//    update_batch bigint
    val rd = new Random()
    val statement=connection.prepareStatement("insert into kudu.mydu.fbm_tort_data_video values(?,?,?,?,?,?)")
    var count = 0
    var current = new Date()
    var ts = new Timestamp(current.getTime)
    for (i<- 0 to 100000000){
      statement.setString(1,UUID.randomUUID().toString)
      statement.setString(2,"dep_"+rd.nextInt(3000))
      statement.setString(3,"")
      statement.setString(4,"2020-01-02")
      statement.setTimestamp(5,ts)
      statement.setLong(6,current.getTime)
      statement.execute()
      if(count % 100 ==0){
        connection.commit()
        sendMsg(topic,"kudu.mydu.fbm_tort_data_video;" + current.getTime)
        current = new Date()
        ts = new Timestamp(current.getTime)
        Thread.sleep(20)
      }
    }
    connection.commit()
  }

  val props = new Properties
  props.put("bootstrap.servers", "m.zy.com:9092")
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)
  //指定topic名称
  val topic = "ods_batch"
  //create kafka connection
  val producer = new KafkaProducer[String, String](props)

  def sendMsg(topic:String,message:String): Unit ={
    producer.send(new ProducerRecord[String, String](topic, message))
  }
}
