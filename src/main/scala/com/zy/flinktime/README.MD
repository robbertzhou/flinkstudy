一、时间类型  
1、event time：event产生的时间  
2、ingestion time：Source进入到flink的时间  
3、process time：window窗口处理时间。  
二、如何处理乱序事件  
Window的设定与数据无光。  
watermark时间>=eventTime，在【start_time,end_time)有数据。