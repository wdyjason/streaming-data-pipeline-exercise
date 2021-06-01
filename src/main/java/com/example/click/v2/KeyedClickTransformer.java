package com.example.click.v2;

import com.example.click.Click;
import com.example.click.RawClick;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedClickTransformer {
    public KeyedClickTransformer(DataStreamSource<RawClick> stream) {
        this.stream = stream;
    }

    private final DataStreamSource<RawClick> stream;

    public SingleOutputStreamOperator<Click> perform() {
        return stream
                .process(new ProcessFunction<RawClick, Click>() {
                    @Override
                    public void processElement(RawClick raw, Context ctx, Collector<Click> out) {
                        Click record = new Click(raw.getItemId(), raw.getCount(), ctx.timerService().currentProcessingTime());
                        out.collect(record);
                    }
                })
                .keyBy(Click::getItemId)
                .reduce((ReduceFunction<Click>) (value1, value2) -> new Click(value1.getItemId(), value1.getCount() + value2.getCount(), value2.getTimestamp()));
    }
}
