package com.zy.sinks

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.kudu.client.KuduClient

class KuduSink extends RichSinkFunction[String]{
  val client = new KuduClient.KuduClientBuilder("master.zy.com:7051").build

  override def invoke(value: String): Unit = {
    val po = JSON.parseObject(value)

  }
}
