package com.zy.datastreamoperate;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

/**
 * map
 * 元素一对一，且datastream也是一对一
 */
public class MapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> src = env.fromElements(1,3,4,5);
        DataStream<Integer> maped = src.map(tt -> tt+ 1);
        maped.print();
       env.execute();
    }
}
