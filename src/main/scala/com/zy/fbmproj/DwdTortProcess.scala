package com.zy.fbmproj

import java.sql.{Connection, DriverManager}
import java.{lang, util}
import java.util.Properties

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.collection.mutable

/**
 * create 2020-06-27
 * author zhouyu
 * desc
 */
object DwdTortProcess {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val topic = "ods_batch"
    val props = new Properties
    props.setProperty("bootstrap.servers", "m.zy.com:9092")
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
    val fm = data.flatMap(rec =>{
      Class.forName("com.facebook.presto.jdbc.PrestoDriver")
      val split = rec.toString.split(";")
      val connection = DriverManager.getConnection("jdbc:presto://m.zy.com:8081/kudu/default","hive",null)
      val query = connection.prepareStatement("select unique_key,dep_id,dim_date from kudu.mydu.fbm_tort_data_video " +
        "where update_batch = " + split(1))
//      val insertDwd =connection.prepareStatement("insert into kudu.dwd.fact_tort values(?,?,?,?)")
      val rs = query.executeQuery()
      val list = new util.ArrayList[TEntity]()
      while (rs.next()){
        val uk = rs.getString(1)
        val depId = rs.getString(2)
        val depName = queryDepName(connection,depId)
        val dimDate = rs.getString(3)
        val entity = new TEntity()
        entity.setUnqiueKey(uk)
        entity.setDepId(depId)
        entity.setDepName(depName)
        entity.setDimDate(dimDate)
        list.add(entity)
      }
      list.toArray()
    }).map(r => {
      val cls = r.getClass
      val unk = cls.getField("unqiueKey").get(r).toString
      val did = cls.getField("depId").get(r).toString
      val dname = cls.getField("depName").get(r).toString
      val dimDate = cls.getField("dimDate").get(r).toString
      (unk,did,dname,dimDate)
    })
    fm.timeWindowAll(Time.seconds(5))
      .process(new ProcessAllWindowFunction[(String,String,String,String), mutable.Map[String, Int], TimeWindow] {
        override def process(context: Context, elements: Iterable[(String, String, String, String)], out: Collector[mutable.Map[String, Int]]): Unit = {
          val connection = DriverManager.getConnection("jdbc:presto://m.zy.com:8081/kudu/default","hive",null)
          connection.setAutoCommit(false)
          val insertDwd =connection.prepareStatement("insert into kudu.dwd.fact_tort values(?,?,?,?)")
          val it = elements.toIterator
          while(it.hasNext){
            val entity = it.next()
            insertDwd.setString(1,entity._1)
            insertDwd.setString(2,entity._2)
            insertDwd.setString(3,entity._3)
            insertDwd.setString(4,entity._4)
            insertDwd.execute()
          }
          connection.commit()
        }
      })

    env.execute("insert dwd job")
  }

  def queryDepName(conn:Connection,depId:String) :String={
    val query =conn.prepareStatement("select dep_name from kudu.mydu.deps where dep_id = '" + depId + "'")
    val rs = query.executeQuery()
    while(rs.next()){
      return rs.getString(1)
    }
    return ""
  }
}
