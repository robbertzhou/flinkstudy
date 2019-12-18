4种常用的API：  
1、低级API：提供时间细粒度的控制。易用性和简洁性差。  
2、核心API：提供了对离线和批处理数据。对低级API进行了封装，提供了filter、sum  
等高级函数，简单易用。  
3、Table API：与datastream和dataset一起用。  
4、SQL：flink的sql集成是基于apache calcite的。