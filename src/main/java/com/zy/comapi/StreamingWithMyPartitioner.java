package com.zy.comapi;


import com.zy.commonapi.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingWithMyPartitioner {
    static Logger logger = LoggerFactory.getLogger(StreamingWithMyPartitioner.class);

    public static void main(String[] args) throws Exception{
//        BasicConfigurator.configure();
        PropertyConfigurator.configure("/home/app/flink-1.12.0/log4j.propeties");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Object> text = env.addSource(new NoParallelSource());
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Object, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Object value) throws Exception {
                return new Tuple1<Long>(Long.parseLong(value.toString()));
            }
        });
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new MyPartitioner(),0);
        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                logger.info("当前线程id:" + Thread.currentThread().getId());
                return value.getField(0);
            }
        });
        result.print();
        env.execute("a");
    }
}
