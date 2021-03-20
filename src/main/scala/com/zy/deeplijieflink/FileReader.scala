package com.zy.deeplijieflink

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode


object FileReader {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val filePath = "i:\\testdata\\textfilesource.txt"
    val format = new TextInputFormat(new Path(filePath))
    val dataStream = env.readFile(format,filePath,FileProcessingMode.PROCESS_CONTINUOUSLY,1000)
    dataStream.print("intsmaze--")
    env.execute("sd")
  }
}
