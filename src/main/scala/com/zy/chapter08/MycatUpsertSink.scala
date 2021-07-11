package com.zy.chapter08

import java.sql.{Connection, PreparedStatement}

import com.zy.chapter05.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * create 2020-02-28
 * author zhouyu
 * desc mycat的幂等性操作
 */
class MycatUpsertSink extends RichSinkFunction[SensorReading]{
  var connection : Connection = _
  var insertStatement : PreparedStatement = _
  var updateStatement : PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    import java.sql.DriverManager
    connection = DriverManager.getConnection("jdbc:mysql://mymaster:8066/FIRSTDB?useSSL=false&serverTimezone=UTC", "user", "user")
    insertStatement = connection.prepareStatement("insert into temperature(sensor,temp) values(?,?)")
    updateStatement = connection.prepareStatement("update temperature set temp = ? where sensor = ?")
  }

//  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
//    updateStatement.setDouble(1,value.temperature)
//    updateStatement.setString(2,value.id)
//    updateStatement.execute()
//    if(updateStatement.getUpdateCount ==0){
//      insertStatement.setString(1,value.id)
//      insertStatement.setDouble(2,value.temperature)
//      insertStatement.execute()
//    }
//  }

//  override def close(): Unit = {
//    insertStatement.close()
//    updateStatement.close()
//    connection.close()
//  }
}
