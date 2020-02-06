package com.zy.chapter06

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * @create 2020-01-28
  * @author zhouyu
  * @desc 聚合窗口函数(IN,ACC,OUT)
  */
class AvgTempFunction  extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double)]{
  override def createAccumulator(): (String, Double, Int) = {
    ("",0,0)
  }

  override def add(value: (String, Double), accumulator: (String, Double, Int)): (String, Double, Int) = {
    (value._1,value._2+accumulator._2,1+accumulator._3)
  }

  override def getResult(accumulator: (String, Double, Int)): (String, Double) = {
    (accumulator._1,accumulator._2 / accumulator._3)
  }

  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = {
    (a._1,a._2+b._2,a._3 + b._3)
  }
}
