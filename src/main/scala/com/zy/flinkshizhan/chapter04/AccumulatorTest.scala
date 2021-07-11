package com.zy.flinkshizhan.chapter04

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class AccumulatorTest extends RichMapFunction[Int,String]{
  val numLines = new IntCounter()

  override def open(parameters: Configuration): Unit = {
    
  }

  override def map(in: Int): String = {

    ""
  }
}
