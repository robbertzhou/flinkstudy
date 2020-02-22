package com.zy.javarealtimereport;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/***
 * @create 2020-02-06
 * @author zhouyu
 * @desc 实时报表入口程序
 */
public class DataReport {
    private static Logger logger = LoggerFactory.getLogger(DataReport.class);
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //set checkpoint
        env.enableCheckpointing(60000L);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setMinPauseBetweenCheckpoints(30000);
        config.setCheckpointTimeout(10000);
        config.setMaxConcurrentCheckpoints(1);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://master.zy.com:8020/tmp"));

        /**
         * 配置kafka source
         */
        String topic = "auditLog";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","master.zy.com:9092");
        props.setProperty("group.id","con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(topic,new SimpleStringSchema(),props);

        /**
         * 获取kafka数据
         * 格式：//{"dt":"审核时间[年月日 时分秒']","type":"审核类型","username":"审核人姓名","area":"大区"}
         */
        DataStreamSource<String> data = env.addSource(myConsumer);

        /**
         * 对数据进行清洗
         */
        DataStream<Tuple3<Long,String,String>> mapData = data.map(new DataCleanFunction());

        /**
         * 过滤异常数据
         */
        DataStream<Tuple3<Long,String,String>> filterData =  mapData.filter(new FilterAuditData());
        //保存延迟太久的数据
        OutputTag<Tuple3<Long,String,String>> outputTag = new OutputTag<Tuple3<Long,String,String>>("late-data"){};

//        KeyedStream<Tuple3<Long,String,String>,Tuple> rs = filterData.keyBy(1,2);
//                .assignTimestampsAndWatermarks(new MyWatermark());
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> resultData =  filterData
                .assignTimestampsAndWatermarks(new MyWatermark())
                .keyBy(1,2)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(6))
                .sideOutputLateData(outputTag)
                .apply(new MyAggFunction())
                ;

//        /**
//         * 窗口操作统计
//         */
//        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> resultData =
//                filterData
//                        .assignTimestampsAndWatermarks(new MyWatermark())
//                .keyBy(1,2)
//                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
////                .allowedLateness(Time.seconds(6))
////                .sideOutputLateData(outputTag)
//                .apply(new MyAggFunction());
//
//        //获取延迟数据
        DataStream<Tuple3<Long,String,String>> sideOutput = resultData.getSideOutput(outputTag);
//        /**
//         * 把延迟数据放入kafka中
//         */
        String outTopic = "lateLog";
        Properties propOut = new Properties();
        propOut.setProperty("bootstrap.servers","master.zy.com:9092");
        propOut.setProperty("transaction.timeout.ms","60000");
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(outTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),propOut,FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
//
        sideOutput.map(new SideDataMap()).addSink(myProducer);
//
//        /**
//         * 计算结果保存到ES
//         */

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("master.zy.com",9200,"http"));
        ElasticsearchSink.Builder<Tuple4<String,String,String,Long>> esSinkBuilder =
                new ElasticsearchSink.Builder<>(httpHosts,
                        new DataInsertElasticsearch());
        esSinkBuilder.setBulkFlushMaxActions(1);
        resultData.addSink(esSinkBuilder.build());
//        resultData.print();


        env.execute("DataReport");
    }
}
