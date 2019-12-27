package com.zy.fromsource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * fromColletion从数组
 */
public class FromCollectionTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        String[] names = {"Jack","Tom","Jenny"};
        DataStream<String> src = env.fromCollection(Arrays.asList(names));
        src.print();
        env.execute("jjj");
    }
}
