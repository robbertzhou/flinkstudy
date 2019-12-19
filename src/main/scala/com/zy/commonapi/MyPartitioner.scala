package com.zy.commonapi

import org.apache.flink.api.common.functions.Partitioner

class MyPartitioner extends Partitioner[Long]{
  override def partition(k: Long, numPartitions: Int): Int = {
    println("分区总数：" + numPartitions)
    if(k%2==0){
      0
    }else{
      1
    }
  }
}
