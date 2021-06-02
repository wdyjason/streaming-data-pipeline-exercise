package streamingdatapipelineexercise.examples.click;

import streamingdatapipelineexercise.examples.click.shared.Click;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClickSInkFunction implements SinkFunction<Click> {
    public static final List<Click> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(Click value, Context context) {
        values.add(value);
    }
}
