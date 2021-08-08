package streamingdatapipelineexercise.examples.click.homework;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streamingdatapipelineexercise.examples.click.shared.Click;
import streamingdatapipelineexercise.examples.click.shared.WindowClickRecord;

import java.sql.Timestamp;
import java.time.Instant;


public class ClickWindowFunction extends ProcessWindowFunction<Click, WindowClickRecord, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Click> elements, Collector<WindowClickRecord> out) throws Exception {
        var record = elements.iterator().next();
        TimeWindow window = context.window();
        Timestamp from = Timestamp.from(Instant.ofEpochMilli(window.getStart()));
        Timestamp to = Timestamp.from(Instant.ofEpochMilli(window.getEnd()));
        out.collect(new WindowClickRecord(record.getItemId(), record.getCount(), from, to));
    }
}
