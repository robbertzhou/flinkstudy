package com.zy.javarealtimereport;

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.Map;

/**
 * @create 2020-02-06
 * @author zhouyu
 * @desc 插入到es的sink函数
 */
public class DataInsertElasticsearch implements ElasticsearchSinkFunction<Tuple4<String, String, String, Long>> {

    public IndexRequest createIndexRequest(Tuple4<String, String, String, Long> element){
        Map<String,Object> json = new HashedMap();
        json.put("time",element.f0);
        json.put("type",element.f1);
        json.put("area",element.f2);
        json.put("count",element.f3);
        String id = element.f0.replace(" ","-") + "-" + element.f1
                +element.f2;
        return Requests.indexRequest().index("auditindex")
                .type("audittype")
                .id(id)
                .source(json);
    }

    @Override
    public void process(Tuple4<String, String, String, Long> value, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        requestIndexer.add(createIndexRequest(value));
    }
}
