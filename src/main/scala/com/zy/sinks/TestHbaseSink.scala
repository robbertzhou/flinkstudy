package com.zy.sinks

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.util.Random

/**
 * @create 2020-01-18
 * @author zhouyu
 * @desc 测试hbasesink
 */
object TestHbaseSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.addSource(new RichSourceFunction[TestObject] {
      override def run(sourceContext: SourceFunction.SourceContext[TestObject]): Unit = {
          while(true){
            val rand = new Random()
            val id = rand.nextInt(300000) + ""
            val name=id +"jjjj"
            sourceContext.collect(new TestObject(id,name))
          }
      }

      override def cancel(): Unit = {}
    })
    src.writeUsingOutputFormat(new HBaseRichOutputFormat())
    env.execute("hbasesink")
  }
}
