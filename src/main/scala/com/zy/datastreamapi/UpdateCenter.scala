package com.zy.datastreamapi

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, CoProcessFunction}
import org.apache.flink.util.Collector



class UpdateCenter(broadcastStateDesc:MapStateDescriptor[Integer,(Long,String)]) extends BroadcastProcessFunction[String,(Long,String),String]{
  val bsd = broadcastStateDesc
  override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, (Long, String), String]#ReadOnlyContext, collector: Collector[String]): Unit = {

    import org.apache.flink.api.common.state.ReadOnlyBroadcastState
    val state = readOnlyContext.getBroadcastState(bsd)
    import scala.collection.JavaConversions._
    for (entry <- state.immutableEntries) {
      val bKey = entry.getKey
      val bValue = entry.getValue
      // 根据广播数据进行原数据流的各种处理
    }
    collector.collect(in1)
  }

  override def processBroadcastElement(in2: (Long, String), context: BroadcastProcessFunction[String, (Long, String), String]#Context, collector: Collector[String]): Unit = {
    collector.collect("jjje")
  }
}
