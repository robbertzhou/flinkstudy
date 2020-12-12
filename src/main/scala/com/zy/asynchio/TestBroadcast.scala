package com.zy.asynchio

import java.util.concurrent.TimeUnit

import com.zy.sinks.{Student, StudentSource}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * create 2020-12-12
 */
object TestBroadcast {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val ds1 = env.fromElements("a","b","c")
    //自定义广播数据流
    val broadStream = env.addSource(new RichSourceFunction[Student] {
      override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
        TimeUnit.SECONDS.sleep(1)
        val random = new Random()
        val stu = new Student(3,"name " + random.nextInt(100))
        sourceContext.collect(stu)
//        sourceContext.collect("aabb" + random.nextInt(300))

      }

      override def cancel(): Unit = {

      }
    })
    val configFilter = new MapStateDescriptor[String, Student]("configFilter", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(classOf[Student]))
    val bs  = broadStream.setParallelism(1).broadcast(configFilter)
    val src = env.addSource(new StudentSource())
    val outStream = src.connect(bs).process(new BroadcastProcessFunction[Student, Student, String] {

      override def open(param: Configuration): Unit ={
        keyWords = "init"
      }

      //拦截的关键字
      var keyWords :String = null
      override def processElement(in1: Student, readOnlyContext: BroadcastProcessFunction[Student, Student, String]#ReadOnlyContext, collector: Collector[String]): Unit = {
        println("hello world:" + keyWords)
        collector.collect("jack")
      }

      override def processBroadcastElement(in2: Student, context: BroadcastProcessFunction[Student, Student, String]#Context, collector: Collector[String]): Unit = {
        keyWords = in2.name
      }
    })
    outStream.print()
//    val map = src.map(new RichMapFunction[Student,String] (){
//      override def map(in: Student): String = {
//        println("test data is:")
//        "dd"
//      }
//    })
//    map.print()
    env.execute("d")

  }

}
