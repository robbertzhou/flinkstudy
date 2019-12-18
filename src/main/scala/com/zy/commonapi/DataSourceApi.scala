package com.zy.commonapi

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * 数据源的api
  */
object DataSourceApi {
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    //基于textfile,遵循textInputFormat逐行读取规则并返回
    env.readTextFile("path")
    //基于socket
    env.socketTextStream("host",22)
  }
}
