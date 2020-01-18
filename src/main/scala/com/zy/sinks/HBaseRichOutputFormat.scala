package com.zy.sinks

import java.util

import com.zy.common.HbaseUtil
import org.apache.flink.api.common.io.{OutputFormat, RichOutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

/**
 * @create 2020-01-18
 * @author zhouyu
 * @desc 自定义hbase的输出
 */
class HBaseRichOutputFormat extends org.apache.flink.api.common.io.OutputFormat[TestObject]{
  val logger: Logger = LoggerFactory.getLogger(getClass)
  var table: Table = _

//  override def configure(parameters: Configuration) :Unit= {
//    logger.info("configure open")
//  }
//  override def open(taskNumber: Int, numTasks: Int): Unit = {
//    table = HbaseUtil().connect.getTable(TableName.valueOf("test"))
//  }
//  override def writeRecord(record: Array[(String,Int)]): Unit ={
//    import scala.collection.JavaConverters._
//    //批量写入数据
//    val list = record.map(d=>{
//      val put = new Put(Bytes.toBytes(d._1))
//      put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("name"),Bytes.toBytes(d._2))
//      put
//    }).toList
//    table.put(list.asJava)
//  }
//  override def close()  :Unit= {
//    // 结束的时候记得关闭连接（其实永远不会结束）
//    table.close()
//  }
  override def configure(configuration: Configuration): Unit = {

}

  override def open(i: Int, i1: Int): Unit = {
    table = HbaseUtil().connect.getTable(TableName.valueOf("test"))
  }

  override def writeRecord(to: TestObject): Unit = {
    val put = new Put(to.ii.getBytes())
    put.addColumn("info".getBytes(),"id".getBytes(),to.ii.getBytes())
    put.addColumn("info".getBytes(),"name".getBytes(),to.nn.getBytes())
    table.put(put)
  }

  override def close(): Unit = {
    table.close()
  }
}