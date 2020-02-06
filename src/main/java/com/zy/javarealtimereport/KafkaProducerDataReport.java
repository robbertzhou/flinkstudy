package com.zy.javarealtimereport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.util.PropertiesTrait;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @create 2020-02-06
 * @author zhouyu
 * @desc 模拟kafka发送数据
 */
public class KafkaProducerDataReport {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers","master.zy.com:9092");
        props.put("key.serializer",StringSerializer.class.getName());
        props.put("value.serializer",StringSerializer.class.getName());
        //指定topic名称
        String topic = "auditLog";
        //create kafka connection
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);
        while (true){
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"type\":\""
                    + randomType() + "\",\"username\":\"" + getUserName() + "\",\"area\":\""
                    +getRandomArea() + "\"}";
            producer.send(new ProducerRecord<String,String>(topic,message));
            Thread.sleep(1000);

        }
//        producer.close();
    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getRandomArea(){
        String[] types = {"US","AR","IN","CN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String randomType(){
        String[] types = {"shelf","unshelf","black","child_shelf","child_unshelf"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getUserName(){
        String[] types = {"zs","ls","ww","zl"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
