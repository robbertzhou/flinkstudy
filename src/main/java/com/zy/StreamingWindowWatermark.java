package com.zy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class StreamingWindowWatermark {
    public static void main(String[] args) throws Exception{
        int port = 9000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("192.168.0.121",port,"\n");
        DataStream<Tuple2<String,Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<>(arr[0],Long.parseLong(arr[1]));
            }
        });

        DataStream<Tuple2<String,Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    Long currentMaxtimestamp = 0L;
                    final Long maxOutOfOrderness = 10000L;
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxtimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                        Long timestamp = element.f1;
                        currentMaxtimestamp = Math.max(timestamp,currentMaxtimestamp);
                        System.out.println("key:" + element.f0+",eventtime:" + element.f1 + "|"
                        + sdf.format(element.f1)+"],currentMaxtimestamp=" + currentMaxtimestamp +"|"+
                        sdf.format(currentMaxtimestamp)+"],watermark:[" +
                        getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                        return timestamp;
                    }
                }
        );

        DataStream<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input,
                                      Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<>();
                        Iterator<Tuple2<String,Long>> it = input.iterator();
                        Collections.sort(arrarList);
                        System.out.println("window compute.");
                    }
                });

        window.print();
        env.execute("");
    }
}
