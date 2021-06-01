package com.example.click.v3;

import com.example.click.shared.ClickRecord;
import com.example.click.shared.RawClick;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Instant;

public class KeyedClickByTableTransformer {
    public KeyedClickByTableTransformer(DataStreamSource<RawClick> stream) {
        this.stream = stream;
    }

    private final DataStreamSource<RawClick> stream;

    public SingleOutputStreamOperator<ClickRecord> perform() {
        return stream
                .process(new ProcessFunction<RawClick, ClickRecord>() {
                    @Override
                    public void processElement(RawClick raw, Context ctx, Collector<ClickRecord> out) {
                        var epochMilli = ctx.timerService().currentProcessingTime();
                        Timestamp timestamp = Timestamp.from(Instant.ofEpochMilli(epochMilli));
                        ClickRecord record = new ClickRecord(raw.getItemId(), raw.getCount(), timestamp);
                        out.collect(record);
                    }
                })
                .keyBy(ClickRecord::getItemId)
                .reduce((ReduceFunction<ClickRecord>) (value1, value2) ->
                        new ClickRecord(value1.getItemId(),
                                value1.getCount() + value2.getCount(),
                                value2.getTimestamp())
                );
    }
}
