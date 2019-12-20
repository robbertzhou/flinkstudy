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
    import org.apache.flink.streaming.api._
    val src = env.addSource(new NoParallelSource())
    val tupleData = src.map[Tuple1[Long]](new MapFunction[Long,Tuple1[Long]] {
      override def map(t: Long): Tuple1[Long] = {
        Tuple1(t)
      }
    })

//    tupleData.partitionCustom(new MyPartitioner,0)
    val mm = tupleData.map(new MapFunction[Tuple1[Long],Long]() {
      override def map(value: Tuple1[Long]): Long = {
        value._1
      }
    })
    mm.print()


    env.execute("partition test.")
  }
}
