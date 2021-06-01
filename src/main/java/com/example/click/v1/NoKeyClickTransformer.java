package com.example.click.v1;

import com.example.click.RawClick;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.math.BigInteger;

public class NoKeyClickTransformer {
    public NoKeyClickTransformer(DataStream<String> stream) {
        this.stream = stream;
    }

    private final DataStream<String> stream;

    public SingleOutputStreamOperator<RawClick> perform() {
        return stream.map(value -> {
            var split = value.split(":");
            var itemId = split[0];
            var count = new BigInteger(split[1]);
            return new RawClick(itemId, count.longValue());
        })
                .keyBy(RawClick::getItemId)
                .sum("count");
    }
}
