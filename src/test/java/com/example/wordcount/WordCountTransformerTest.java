package com.example.wordcount;

import com.example.support.TestHelper;
import com.example.wordcount.wordcount.WordCountTransformer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class WordCountTransformerTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = TestHelper.getMiniClusterWithClientResource();

    @Test
    void should_count_words() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        CollectSink.values.clear();
        DataStreamSource<String> dataStreamSource = env.fromElements(
                "Hello world",
                "Hello tomorrow"
        );

        DataStream<Tuple2<String, Integer>> dataStream = new WordCountTransformer(dataStreamSource).perform();

        dataStream.addSink(new CollectSink());

        env.execute();

        assertTrue(CollectSink.values.containsAll(List.of(
                Tuple2.of("Hello", 2),
                Tuple2.of("world", 1),
                Tuple2.of("tomorrow", 1)
        )));
    }

    private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {
        public static final List<Tuple2<String, Integer>> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) {
            values.add(value);
        }
    }
}
