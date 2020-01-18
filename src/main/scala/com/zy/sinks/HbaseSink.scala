package com.zy.sinks

import com.zy.common.HbaseUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.hadoop.hbase.{Cell, TableName}
import org.apache.hadoop.hbase.client.{Put, Table}

/**
  * @create 2020-01-17
  * @author zhouyu
  * @desc hbase的sink器
  */
class HbaseSink extends RichSinkFunction[TestObject]{
  val connection = HbaseUtil.apply().connect
  var table : Table = _

  override def open(parameters: Configuration): Unit = {
    table = connection.getTable(TableName.valueOf("test"))
  }

  override def invoke(value: TestObject): Unit = {
    val id = value.ii
    val put :Put = new Put(id.getBytes)
    put.addColumn("info".getBytes,"id".getBytes(),id.getBytes())
    put.addColumn("info".getBytes,"name".getBytes(),value.nn.getBytes())
    table.put(put)
  }

  override def close(): Unit = {
    connection.close()
  }
}
