package com.zy.commonapi


import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api._




/**
  * 自定义分区测试
  */
object DemoPartitioner {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val src = env.addSource(new NoParallelSource())
    val tupleData = src.map[org.apache.flink.api.java.tuple.Tuple1[Long]](new MapFunction[Long, org.apache.flink.api.java.tuple.Tuple1[Long]] {
      override def map(t: Long): org.apache.flink.api.java.tuple.Tuple1[Long] = {
        val t1 = new org.apache.flink.api.java.tuple.Tuple1[Long]()
        t1.f0 = t
        return t1
      }
    })

    tupleData.partitionCustom(new MyPartitioner,0)



    env.execute("partition test.")
  }
}
