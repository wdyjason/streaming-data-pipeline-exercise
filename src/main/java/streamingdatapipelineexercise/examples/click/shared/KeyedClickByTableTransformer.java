package streamingdatapipelineexercise.examples.click.shared;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Instant;

public class KeyedClickByTableTransformer {
    public KeyedClickByTableTransformer(DataStreamSource<RawClick> stream) {
        this.stream = stream;
    }

    private final DataStreamSource<RawClick> stream;

    public SingleOutputStreamOperator<WindowClickRecord> perform() {
        return stream
                .keyBy(RawClick::getItemId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new MyReduceFunction(), new MyProcessWindowFunction());
    }

    public static class MyReduceFunction implements ReduceFunction<RawClick> {

        @Override
        public RawClick reduce(RawClick value1, RawClick value2) {
            return new RawClick(value1.getItemId(),
                    value1.getCount() + value2.getCount());
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<RawClick, WindowClickRecord, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<RawClick> elements, Collector<WindowClickRecord> out) {
            RawClick record = elements.iterator().next();
            TimeWindow window = context.window();
            Timestamp from = Timestamp.from(Instant.ofEpochMilli(window.getStart()));
            Timestamp to = Timestamp.from(Instant.ofEpochMilli(window.getEnd()));
            out.collect(new WindowClickRecord(record.getItemId(), record.getCount(), from, to));
        }
    }
}
