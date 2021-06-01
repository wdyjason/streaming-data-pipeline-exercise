package com.example.click.v1;

import com.example.click.shared.RawClick;
import com.example.click.RawClickSInkFunction;
import com.example.support.TestHelper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class NoKeyClickTransformerTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            TestHelper.getMiniClusterWithClientResource();

    @Test
    void should_count_click() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // try to increase the parallelism
        RawClickSInkFunction.values.clear();
        DataStreamSource<String> dataStreamSource = env.fromElements(
                "10001:1",
                "10001:1",
                "10002:1"
        );

        SingleOutputStreamOperator<RawClick> perform = new NoKeyClickTransformer(dataStreamSource).perform();

        perform.addSink(new RawClickSInkFunction());

        env.execute();

        List<RawClick> expectedRawClicks = List.of(
                new RawClick("10001", 1),
                new RawClick("10001", 2),
                new RawClick("10002", 1)
        );
        assertArrayEquals(expectedRawClicks.toArray(), RawClickSInkFunction.values.toArray());
    }
}
