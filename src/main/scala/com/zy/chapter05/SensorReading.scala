package com.zy.chapter05

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @create 2020-01-23
  * @author zhouyu
  * @desc 定义一个传感器类
  * @param id
  * @param timestamp
  * @param temperature
  *
  */
class SensorReading(ii: String, ts: Long, tt: Double) extends Serializable {
  var id = ii
  var timestamp : Long= ts
  var temperature = tt

  override def toString: String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    "传感器id=" + id + ",时间：" + sdf.format(new Date(timestamp)) + "，温度为:" + temperature
  }
}
