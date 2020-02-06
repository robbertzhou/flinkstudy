package com.zy.javarealtimereport;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @create 2020-02-06
 * @author zhouyu
 * @desc 报表聚合函数
 */
public class MyAggFunction implements WindowFunction<Tuple3<Long,String,String>,Tuple4<String,String,String,Long>
        ,Tuple,TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Long, String, String>> iterable,
                      Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
        String type = tuple.getField(0).toString();
        String area = tuple.getField(1).toString();
        Iterator<Tuple3<Long, String, String>> it = iterable.iterator();
        ArrayList<Long> arrayList = new ArrayList<>();
        long count = 0;
        while (it.hasNext()){
            Tuple3<Long, String, String> next = it.next();
            arrayList.add(next.f0);
            count++;
        }
        System.out.println("window触发了：" + count +"条数据。");
        Collections.sort(arrayList);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = sdf.format(arrayList.get(arrayList.size() - 1));
        Tuple4<String,String,String,Long> res = new Tuple4<>(time,type,area,count);
        collector.collect(res);
    }
}
