package com.zy.sinks

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * @create 2020-01-17
  * @author zhouyu
  * @desc mysql sink
  */
class MySqlSink extends RichSinkFunction[Object]{
  var ps : PreparedStatement = null
  var connection : Connection = null


  override def open(parameters: Configuration): Unit = {
      connection = getConnection()
    ps = connection.prepareStatement("insert into student values(?,?,?)")

  }

  override def invoke(value: Object): Unit = {
    ps.setString(1,"")
    ps.execute()
  }

  override def close(): Unit = {
    if(connection!=null){
      connection.close()
    }
  }

  def  getConnection() : Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn :Connection= DriverManager.getConnection("jdbc:mysql://lll:3306")
    conn
  }

}
