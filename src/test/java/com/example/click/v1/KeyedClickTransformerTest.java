package com.example.click.v1;

import com.example.click.Click;
import com.example.click.ClickSInkFunction;
import com.example.click.v2.KeyedClickTransformer;
import com.example.support.TestHelper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class KeyedClickTransformerTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            TestHelper.getMiniClusterWithClientResource();

    @Test
    void should_count_click() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        ClickSInkFunction.values.clear();
        DataStreamSource<Click> dataStreamSource = env.fromElements(
                new Click("10001", 1),
                new Click("10001", 1),
                new Click("10002", 1)
        );

        SingleOutputStreamOperator<Click> perform = new KeyedClickTransformer(dataStreamSource).perform();

        perform.addSink(new ClickSInkFunction());

        env.execute();

        List<Click> expectedClicks = List.of(
                new Click("10001", 1),
                new Click("10001", 2),
                new Click("10002", 1)
        );
        assertArrayEquals(expectedClicks.toArray(), ClickSInkFunction.values.toArray());
    }

}
