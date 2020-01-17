package com.zy.sinks

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

class StudentSource extends RichSourceFunction[Student]{
  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    while (true){
      Thread.sleep(20)
      val rand = new Random()
      val uid = rand.nextInt(3000)
      val stu = new Student(uid,"name" + uid)
      ctx.collect(stu)
    }
  }

  override def cancel(): Unit = {

  }
}
