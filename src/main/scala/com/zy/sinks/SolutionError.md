一、hbase sink问题  
1、不能用RichSinkFunction函数，因为hbase客户端有不能序列化的类。  
2、TableName的错误：没有构造函数，要用TableName.valueOf("tabname")  
3、类型匹配问题：TestObject