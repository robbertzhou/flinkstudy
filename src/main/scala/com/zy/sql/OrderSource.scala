package com.zy.sql

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

/**
  * @create 2020-01-17
  * @author zhouyu
  *
  */
class OrderSource extends RichSourceFunction[Order]{
  var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
    while (isRunning){
      Thread.sleep(200)
      val rand = new Random()
      val uid = rand.nextInt(100)
      val product = "prod" + uid
      val amt = rand.nextInt(3000)
      ctx.collect(new Order(uid,product,amt))
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
