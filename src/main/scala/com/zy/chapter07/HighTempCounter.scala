package com.zy.chapter07

import java.{lang, util}

import com.zy.chapter05.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector

/**
  * create 2020-01-29
  * author zhouyu
  * desc 通过ListCheckpointed算子实现算子状态列表
  */
class HighTempCounter(val thresold:Double)
extends RichFlatMapFunction[SensorReading,(Int,Long)]
  with ListCheckpointed[java.lang.Long]
{

  //子任务的索引号
  private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask()
  //本地计数器
  private var highTempCnt = 0L

  override def flatMap(value: SensorReading, out: Collector[(Int,Long)]): Unit = {
    if(value.temperature > thresold){
      highTempCnt += 1
      out.collect((subtaskIdx,highTempCnt))
    }
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Long] = {
    java.util.Collections.singletonList(highTempCnt)
  }

  override def restoreState(state: java.util.List[java.lang.Long]): Unit = {

    import scala.collection.JavaConverters._
    highTempCnt = 0
    for(cnt <- state.asScala){
      highTempCnt += cnt
    }
  }
}
