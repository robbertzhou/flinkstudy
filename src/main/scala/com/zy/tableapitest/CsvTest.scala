package com.zy.tableapitest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}


object CsvTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = TableEnvironment.getTableEnvironment(env)
//    import org.apache.flink.api.common.typeinfo.TypeInformation
//    import org.apache.flink.table.sources.CsvTableSource
////    val csvTableSource = new CsvTableSource(inPath, Array[String]("trans_id", "part_dt", "lstg_format_name", "leaf_categ_id", "lstg_site_id", "slr_segment_cd", "price",
////      "item_count", "seller_id"), Array[TypeInformation[_]](Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.INT, Types.INT, Types.FLOAT, Types.LONG, Types.LONG))
//    val src = new CsvTableSource("file:///g:\\test_data\\stu.csv",Array("a","b","c"),Array[TypeInformation[_]](Types.STRING,Types.STRING,Types.STRING))
//    tableEnv.registerTableSource("stu",src)
//    tableEnv.sqlQuery("select * from stu").printSchema()
    env.execute("ee")
  }
}
