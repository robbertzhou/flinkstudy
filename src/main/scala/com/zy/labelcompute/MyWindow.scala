package com.zy.labelcompute
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows

class MyWindow extends WindowFunction[Long,Long ,String,TimeWindow]{

}
