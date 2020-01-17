package com.zy.sinks

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * @create 2020-01-17
  * @author zhouyu
  * @desc 写入postgresql数据的sink
  */
class PostgresqlSink extends RichSinkFunction[Student]{
    var ps: PreparedStatement = null
  var conn : Connection = null

  override def open(parameters: Configuration): Unit = {
    conn = getConnect()
    ps = conn.prepareStatement("insert into student values(?,?)")
  }


  override def invoke(value: Student): Unit = {
      ps.setInt(1,value.id)
    ps.setString(2,value.name)
    ps.execute()
  }

  override def close(): Unit = {
    conn.close()
  }


  def getConnect():Connection = {
    Class.forName("org.postgresql.Driver")
    conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/test","dbuser","123")
    conn
  }
}

object PostgresqlSink {
  def main(args: Array[String]): Unit = {
    val ps = new PostgresqlSink()
    ps.open(null)
    val stu = new Student(1,"jack")
    ps.invoke(stu)
  }
}
