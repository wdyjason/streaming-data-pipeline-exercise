package streamingdatapipelineexercise.examples.click.v2;

import streamingdatapipelineexercise.examples.click.shared.Click;
import streamingdatapipelineexercise.examples.click.ClickSInkFunction;
import streamingdatapipelineexercise.examples.click.shared.RawClick;
import streamingdatapipelineexercise.support.TestHelper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KeyedClickTransformerTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            TestHelper.getMiniClusterWithClientResource();

    @Test
    void should_count_click() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // try to increase the parallelism
        ClickSInkFunction.values.clear();
        DataStreamSource<RawClick> dataStreamSource = env.fromElements(
                new RawClick("10001", 1),
                new RawClick("10001", 1),
                new RawClick("10002", 1)
        );

        SingleOutputStreamOperator<Click> perform = new KeyedClickTransformer(dataStreamSource).perform();

        perform.addSink(new ClickSInkFunction());

        env.execute();

        assertEquals(3, ClickSInkFunction.values.size());
        assertEquals("10001", ClickSInkFunction.values.get(0).getItemId());
        assertEquals(1, ClickSInkFunction.values.get(0).getCount());

        assertEquals("10001", ClickSInkFunction.values.get(1).getItemId());
        assertEquals(2, ClickSInkFunction.values.get(1).getCount());

        assertEquals("10002", ClickSInkFunction.values.get(2).getItemId());
        assertEquals(1, ClickSInkFunction.values.get(2).getCount());
    }

}
