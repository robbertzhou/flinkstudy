package com.zy.javarealtimereport;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @create 2020-02-06
 * @author zhouyu
 * @desc 审核数据清洗，从kafka source接收数据，进行map转换
 */
public class DataCleanFunction implements MapFunction<String, Tuple3<Long, String, String>> {
    @Override
    public Tuple3<Long, String, String> map(String line) throws Exception {
        JSONObject jsonObject = JSON.parseObject(line);
        String dt = jsonObject.getString("dt");
        long time = 0;
        try{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parse = sdf.parse(dt);
            time = parse.getTime();
        }catch (Exception ex){
            ex.printStackTrace();
        }
        String type = jsonObject.getString("type");
        String area = jsonObject.getString("area");
         return new Tuple3<>(time,type,area);
    }
}
