package com.zy

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
  * flink mysql
  */
object FlinkSql {
  def main(args: Array[String]): Unit = {

    val rowType = new org.apache.flink.api.java.typeutils.RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    val jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://192.168.0.126:3306/sakila")
      .setUsername("root")
      .setPassword("mima")
      .setQuery("select first_name,last_name,email from customer")
      .setRowTypeInfo(rowType)
      .finish()

    val env = ExecutionEnvironment.getExecutionEnvironment
    val source = env.createInput(jdbcInputFormat)
    val tableEnv : BatchTableEnvironment= new BatchTableEnvironment(env,TableConfig.DEFAULT)
    tableEnv.registerDataSet("customer",source)
    val table = tableEnv.sqlQuery("select * from customer")
    val ds = tableEnv.toDataSet[Row](table)
    ds.print()
  }


}
