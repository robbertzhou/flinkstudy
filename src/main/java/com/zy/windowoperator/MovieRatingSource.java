package com.zy.windowoperator;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.expressions.Rand;

import java.util.Random;

public class MovieRatingSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        //    userId, movieId, rating, timestamp
        while(true){
            try{
                Random rand = new Random();
                for (int i=0;i<10;i++){

                    int uid = rand.nextInt(2);
                    int mid = rand.nextInt(100) + 200;
                    int rating = rand.nextInt(5);
                    sourceContext.collect(uid+"," + mid + "," +rating + "," + System.currentTimeMillis());
                    Thread.sleep(5);
                }

            }catch (Exception ex){

            }
        }
    }

    @Override
    public void cancel() {

    }
}
