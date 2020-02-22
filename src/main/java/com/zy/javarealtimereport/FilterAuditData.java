package com.zy.javarealtimereport;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @create 2020-02-06
 * @author zhouyu
 * @desc 过滤数据（基于业务规则）
 */
public class FilterAuditData implements FilterFunction<Tuple3<Long, String, String>> {
    @Override
    public boolean filter(Tuple3<Long, String, String> value) throws Exception {
        boolean flag = true;
        if(value.f0 == 0){
            flag = false;
        }
        return flag;
    }
}
