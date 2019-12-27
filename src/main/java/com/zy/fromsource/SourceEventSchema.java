package com.zy.fromsource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SourceEventSchema implements DeserializationSchema<SourceEvent> {
    @Override
    public SourceEvent deserialize(byte[] bytes) throws IOException {
        String str = new String(bytes);
        return new SourceEvent();
    }

    @Override
    public boolean isEndOfStream(SourceEvent sourceEvent) {
        return false;
    }

    @Override
    public TypeInformation<SourceEvent> getProducedType() {
        return TypeInformation.of(SourceEvent.class);
    }
}
