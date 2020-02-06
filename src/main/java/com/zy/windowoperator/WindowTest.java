package com.zy.windowoperator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.addSource(new MovieRatingSource(), TypeInformation.of(String.class));
        source.print();
        DataStream<MovieRate> maped = source.map(new MapFunction<String, MovieRate>() {
            @Override
            public MovieRate map(String value) throws Exception {
                MovieRate mr = new MovieRate();
                String[] split = value.split(",");
                mr.setUserId(Integer.parseInt(split[0]));
                mr.setRating(Integer.parseInt(split[2]));
                return mr;
            }
        });
        DataStream<Tuple2<Integer,Double>> rates = maped.keyBy(MovieRate::getUserId).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<MovieRate, AverageAccumulator, Tuple2<Integer,Double>>() {
                    @Override
                    public AverageAccumulator createAccumulator() {
                        return new AverageAccumulator();
                    }

                    @Override
                    public AverageAccumulator add(MovieRate value, AverageAccumulator accumulator) {
                        accumulator.userId = value.getUserId();
                        accumulator.sum += value.getRating();
                        accumulator.count++;
                        return accumulator;
                    }

                    @Override
                    public Tuple2<Integer, Double> getResult(AverageAccumulator accumulator) {
                        return Tuple2.of(accumulator.userId,accumulator.sum / (double)accumulator.count);
                    }

                    @Override
                    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
                        a.count += b.count;
                        a.sum +=b.count;
                        return a;
                    }
                });
        rates.print();
        env.execute("job");
    }

    public static class AverageAccumulator{
        int userId;
        int count;
        double sum;
    }
}
