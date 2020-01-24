package com.zy.chapter05

/**
  * @create 2020-01-23
  * @author zhouyu
  * @desc 定义一个传感器类
  *
  * @param id
  * @param timestamp
  * @param temperature
  *
  */
class SensorReading(ii: String, ts: Long, tt: Double) extends Serializable {
  var id = ii
  var timestamp : Long= ts
  var temperature = tt
}
