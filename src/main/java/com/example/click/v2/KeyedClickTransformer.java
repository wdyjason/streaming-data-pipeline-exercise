package com.example.click.v2;

import com.example.click.Click;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class KeyedClickTransformer {
    public KeyedClickTransformer(DataStreamSource<Click> stream) {
        this.stream = stream;
    }

    private final DataStreamSource<Click> stream;

    public SingleOutputStreamOperator<Click> perform() {
        return stream
                .keyBy(Click::getItemId)
                .reduce((ReduceFunction<Click>) (value1, value2) -> new Click(value1.getItemId(), value1.getCount() + value2.getCount()));
    }
}
