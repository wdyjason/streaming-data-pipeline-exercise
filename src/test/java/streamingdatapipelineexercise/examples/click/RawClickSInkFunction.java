package streamingdatapipelineexercise.examples.click;

import streamingdatapipelineexercise.examples.click.shared.RawClick;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RawClickSInkFunction implements SinkFunction<RawClick> {
    public static final List<RawClick> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(RawClick value, Context context) {
        values.add(value);
    }
}
