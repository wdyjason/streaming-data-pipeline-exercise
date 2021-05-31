package com.example.wordcount.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class WordCountTransformer {
    private DataStreamSource<String> dataStreamSource;

    public WordCountTransformer(DataStreamSource<String> dataStreamSource) {
        this.dataStreamSource = dataStreamSource;
    }

    public DataStream<Tuple2<String, Integer>> perform() {
        return dataStreamSource
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);
    }
}
