package com.zy.asynchio

import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

class MyFunction extends AsyncFunction[String,String]{
  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {

  }
}
