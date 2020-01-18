package com.zy.common

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
 * @create 2020-01-18
 * @author zhouyu
 * @desc hbase的util工具
 */
class HbaseUtil(getConnect:()=>Connection) extends Serializable {
  lazy val connect = getConnect()
}

/**
 * 伴生对象
 */
object HbaseUtil{
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum","master.zy.com,slave1.zy.com,slave2.zy.com")
  conf.set("hbase.zookeeper.property.clientPort","2181")

  def apply(): HbaseUtil ={
    val f=() =>{
      val connection = ConnectionFactory.createConnection(conf)
      //释放资源 在executor的JVM关闭之前,千万不要忘记
      sys.addShutdownHook {
        connection.close()
      }
      connection
    }
    new HbaseUtil(f)
  }
}
