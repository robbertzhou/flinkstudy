package com.zy.javarealtimereport;


import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;


import java.util.ArrayList;
import java.util.List;

/**
 * @create 2020-02-06
 * @author zhouyu
 * @desc es写入练习
 */
public class ESSink {
    public static void main(String[] args) throws Exception{
        org.apache.http.concurrent.FutureCallback ll;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple4<String,String,String,Long>> resultData = env
                .fromElements(
                        new Tuple4<>("jj","kk","kk",11L)
                );
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("master.zy.com",9200,"http"));
        ElasticsearchSink.Builder<Tuple4<String,String,String,Long>> esSinkBuilder =
                new ElasticsearchSink.Builder<>(httpHosts,
                        new DataInsertElasticsearch());
        esSinkBuilder.setBulkFlushMaxActions(1);
        resultData.addSink(esSinkBuilder.build());
        env.execute("te");
    }
}
