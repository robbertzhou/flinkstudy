package com.zy.datastreamapi

import java.sql.DriverManager
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.kudu.client.{KuduClient, KuduTable}

/**
 * create 2020-05-31
 * author zhouyu
 * desc
 */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    implicit val typeInfo = TypeInformation.of(classOf[(Long,String)])
    implicit val typeInfo1 = TypeInformation.of(classOf[String])
    //broadcast 也是一个流
    val stream  = env.addSource(new KuduSource)
    //广播状态的描述符,广播流只支持MapState的结构
    val broadcastStateDescritor = new MapStateDescriptor[Integer,(Long,String)]("centers",classOf[Integer],classOf[(Long,String)])
    val centersBroadcast = stream.broadcast(broadcastStateDescritor)
    /**
     * 配置kafka source
     */
    val topic = "student_topic"
    val props = new Properties
    props.setProperty("bootstrap.servers", "sl1.zy.com:9092")
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
    val tmp = data.connect(centersBroadcast).process(new UpdateCenter(broadcastStateDescritor))
    tmp.print()
    env.execute("broadcast test")
  }

  def loadKuduDeps(): Unit ={
    val client = new KuduClient.KuduClientBuilder("m.zy.com").defaultAdminOperationTimeoutMs(600000).build
    // 获取table
//    com.cloudera.impala.jdbc41.Driver
    val table = client.openTable("impala::test.deps")
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    val conn = DriverManager.getConnection("jdbc:impala://m.zy.com:21050/test")
    val ps = conn.prepareStatement("select * from deps")
    val rs = ps.executeQuery
    while (rs.next()){
      val id = rs.getLong(1)
      val name = rs.getString(2)
    }
  }
}
