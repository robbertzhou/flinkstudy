package com.zy.work;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kudu.client.*;

import java.util.Arrays;
import java.util.Properties;

public class Testkafka {
    public static void main(String[] args) throws Exception{
//
//        create TABLE mysql_json_data
//                (
//                        dbname string,
//                        tablename string,
//                        ts bigint,
//                        type_name string,
//                        xoffset bigint,
//                        data_content string,
//                        folder string,
//                        PRIMARY KEY (dbname,tablename,ts)
//                )

        KuduClient client = new KuduClient.KuduClientBuilder("master.zy.com").defaultAdminOperationTimeoutMs(600000).build();
        // 获取table
        KuduTable table = client.openTable("mysql_json_data");

        // 获取一个会话
        KuduSession session = client.newSession();
        session.setTimeoutMillis(60000);
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setMutationBufferSpace(10000);

        Properties props = new Properties();
        props.put("bootstrap.servers", "slave1.zy.com:9092");
        props.put("group.id", "sse");
        props.put("client.id", "test1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        int count =0;
        try{
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("test1"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    count ++;
                    Upsert upsert = table.newUpsert();
                    PartialRow row = upsert.getRow();
                    JSONObject obj = JSONObject.parseObject(record.value());
//                    dbname string,
//                        tablename string,
//                        ts bigint,
//                        type_name string,
//                        xoffset bigint,
//                        data_content string,
//                        folder string,
                    row.addString(0,obj.getString("database"));
                    row.addString(1,obj.getString("table"));
                    String ts = obj.getString("ts");
                    ts = (ts == null || ts.equals(""))?"0":ts;
                    row.addLong(2,Long.parseLong(ts));
                    row.addString(3,obj.getString("type"));
                    String xoffset = obj.getString("xoffset");
                    xoffset = (xoffset == null || xoffset.equals(""))?"0":xoffset;
                    try{
                        row.addLong(4,Long.parseLong(obj.getString("xoffset")));
                    }catch (Exception ee){
                        row.addLong(4,0L);
                    }

                    row.addString(5,obj.getString("data"));
                    session.apply(upsert);
                    if(count % 500 == 0){
                        session.flush();
                    }

                }

            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
