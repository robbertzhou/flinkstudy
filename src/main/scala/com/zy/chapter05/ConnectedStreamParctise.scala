package com.zy.chapter05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.{CoMapFunction, CoProcessFunction}
import org.apache.flink.util.Collector


/**
  * @create 2020-01-26
  * @author zhouyu
  * @desc 练习Connected stream
  */
object ConnectedStreamParctise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val strSource = env.fromElements(Tuple1("hello"),Tuple1("World"))
    val intSource = env.fromElements(Tuple1(1),Tuple1(2))
    val connnectedStream = strSource.connect(intSource).keyBy("*","*")
//    val processed = connnectedStream.process(new MyProcessFunction())
//    processed.print()
    val maped = connnectedStream.map(new TupleFunction)
    maped.print()
    env.execute("connected practise")

  }
}

class TupleFunction extends CoMapFunction[Tuple1[String],Tuple1[Int],Tuple2[Int,String]]{
  override def map1(value: Tuple1[String]): (Int, String) = {
    Tuple2(1,value._1)
  }

  override def map2(value: Tuple1[Int]): (Int, String) = {
    Tuple2(value._1,"world")
  }
}

class MyProcessFunction extends CoProcessFunction[String,Int,(Int, String)]{
  override def processElement1(value: String, ctx: CoProcessFunction[String, Int, (Int, String)]#Context,
                               out: Collector[(Int, String)]): Unit = {
//    ctx.timestamp()5
    out.collect(Tuple2(10,"jack"))
  }

  override def processElement2(value: Int, ctx: CoProcessFunction[String, Int, (Int, String)]#Context,
                               out: Collector[(Int, String)]): Unit = {
    out.collect(Tuple2(value,"jack"))
  }
}

class MyMapFunction extends CoMapFunction[String,Int,Tuple2[String,Int]]{
  override def map1(value: String): (String, Int) = {
    Tuple2(value,10)
  }

  override def map2(value: Int): (String, Int) = {
    Tuple2("jack",value)
  }
}
