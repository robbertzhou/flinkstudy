package com.zy.javarealtimereport;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;


/**
 * @create 2020-02-06
 * @author zhouyu
 * @desc 延迟数据保存到kafka前的转换
 */
public class SideDataMap implements MapFunction<Tuple3<Long,String,String>,String> {
    @Override
    public String map(Tuple3<Long, String, String> out) throws Exception {
        return out.f0 + "," + out.f1 + "," + out.f2;
    }
}
