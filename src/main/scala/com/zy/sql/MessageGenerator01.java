package com.zy.sql;



import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.time.DateUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @description 电商场景实战之实时PV和UV曲线的数据模拟
 * @author: ZhiWen
 * @create: 2020-01-19 10:56
 **/
public class MessageGenerator01 {



    /**
     * 位置
     */
    private static String[] position = new String[]{"北京","天津","抚州"};


    /**
     *网络方式
     */
    private static String[] networksUse = new String[]{"4G","2G","WIFI","5G"};

    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入","百度跳转","360搜索跳转","必应跳转"};

    /**
     * 浏览器
     */
    private static String[] brower = new String[]{"火狐浏览器","qq浏览器","360浏览器","谷歌浏览器"};

    /**
     * 客户端信息
     */
    private static String[] clientInfo = new String[]{"Android","IPhone OS","None","Windows Phone","Mac"};

    /**
     * 客户端IP
     */
    private static String[] clientIp = new String[]{"172.24.103.101","172.24.103.102","172.24.103.103","172.24.103.104","172.24.103.1","172.24.103.2","172.24.103.3","172.24.103.4","172.24.103.5","172.24.103.6"};

    /**
     * 埋点链路
     */
    private static String[] gpm = new String[]{"http://172.24.103.102:7088/controlmanage/platformOverView","http://172.24.103.103:7088/datamanagement/metadata","http://172.24.103.104:7088/projectmanage/projectList"};

     /*
	 account_id                        VARCHAR,--用户ID。
    client_ip                         VARCHAR,--客户端IP。
    client_info                       VARCHAR,--设备机型信息。
    platform                          VARCHAR,--系统版本信息。
    imei                              VARCHAR,--设备唯一标识。
    `version`                         VARCHAR,--版本号。
    `action`                          VARCHAR,--页面跳转描述。
    gpm                               VARCHAR,--埋点链路。
    c_time                            VARCHAR,--请求时间。
    target_type                       VARCHAR,--目标类型。
    target_id                         VARCHAR,--目标ID。
    udata                             VARCHAR,--扩展信息，JSON格式。
    session_id                        VARCHAR,--会话ID。
    product_id_chain                  VARCHAR,--商品ID串。
    cart_product_id_chain             VARCHAR,--加购商品ID。
    tag                               VARCHAR,--特殊标记。
    `position`                        VARCHAR,--位置信息。
    network                           VARCHAR,--网络使用情况。
    p_dt                              VARCHAR,--时间分区天。
    p_platform                        VARCHAR --系统版本信息。
  * */

    public static void main(String[] args) {

        //配置信息
        Properties props = new Properties();
        //kafka服务器地址
        props.put("bootstrap.servers", "other:9092");
        //设置数据key和value的序列化处理类
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        //创建生产者实例
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);



        //每 1 秒请模拟求一次
        Random random = new Random();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        while(true){

            Message01 message01 = new Message01();
//            message01.setAccount_id(UUID.randomUUID().toString());
//            message01.setClient_ip("172.24.103."+random.nextInt(255));
//            message01.setClient_info(clientInfo[random.nextInt(clientInfo.length)]);
//            message01.setAction(sources[random.nextInt(sources.length)]);
//            message01.setGpm(gpm[random.nextInt(gpm.length)]);
//            message01.setC_time(System.currentTimeMillis()/1000);
//            message01.setUdata("json格式扩展信息");
//            message01.setPosition(position[random.nextInt(position.length)]);
//            message01.setNetwork(networksUse[random.nextInt(networksUse.length)]);
//            message01.setP_dt(sdf.format(new Date()));

            String json = JSONObject.toJSONString(message01);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ProducerRecord record = new ProducerRecord<String, String>("topic_uv",json);
            //发送记录
            producer.send(record);
            System.out.println(json);


        }
    }
}
