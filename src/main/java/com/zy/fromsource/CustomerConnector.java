package com.zy.fromsource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomerConnector implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
