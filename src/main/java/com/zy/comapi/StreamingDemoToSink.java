package com.zy.comapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class StreamingDemoToSink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("192.168.0.125",9000,"\n");
        DataStream<Tuple2<String,String>> maped = text.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("words",value);
            }
        });
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("192.168.0.118").setPort(6379).build();
        RedisSink<Tuple2<String,String>> redisSink = new RedisSink<>(config,new RedisMapperWords());
        maped.addSink(redisSink);
        env.execute("redissink");
    }
}
