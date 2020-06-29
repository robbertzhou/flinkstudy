package com.zy.fbmproj

import java.sql.DriverManager

/**
 * create 2020-06-27
 * author zhouyu
 * desc 插入kudu
 */
object PrestoInsertKudu {
  def main(args: Array[String]): Unit = {
    Class.forName("com.facebook.presto.jdbc.PrestoDriver");
    val connection = DriverManager.getConnection("jdbc:presto://m.zy.com:8081/kudu/default","hive",null)
    connection.setAutoCommit(false)
    val statement=connection.prepareStatement("insert into kudu.mydu.deps values(?,?)")
    var count = 0
    for (i<- 2000000 to 100000000){
      statement.setString(1,"dep_"+i)
      statement.setString(2,"dep_name_"+i)
      statement.execute()
      if(count % 4000 ==0){
        connection.commit()
      }
    }
    connection.commit()


  }
}
