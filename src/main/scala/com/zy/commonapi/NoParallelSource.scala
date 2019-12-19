package com.zy.commonapi

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class NoParallelSource extends SourceFunction[Long]{
  var count = 1L
  var isRunning = true
  override def run(sourceContext: SourceContext[Long]): Unit = {
    while(isRunning){
      count+=1
      sourceContext.collect(count)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
