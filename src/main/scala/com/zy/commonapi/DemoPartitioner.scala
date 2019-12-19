package com.zy.commonapi


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * 自定义分区测试
  */
object DemoPartitioner {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val src = env.addSource(new NoParallelSource)

    val tupleData = src.map(
      line => {
      Tuple1(line)
    }).print()


//    maped.partitionCustom(new MyPartitioner(),1).print()
    env.execute("partition test.")
  }
}
