package com.zy.comapi;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        if(key % 2 ==0){
            return 0;
        }else{
            return 1;
        }
    }
}
