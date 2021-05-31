package com.example.click.v2;

import com.example.click.Click;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


/**
 * Create the topic first:
 * kafka-topics --create --topic keyed_click --bootstrap-server localhost:9092
 *
 * Produce message with
 * kafka-console-producer --topic keyed_click --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:"
 *
 * And messages like (key and value separated by ":"):
 * 100:1
 * 100:1
 * 100:2
 * 101:1
 */

public class KeyedClick {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:19092");
        properties.setProperty("group.id", "KeyedClick");
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var schema = new KeyedClickDeserializationSchema();
        var stream = env
                .addSource(new FlinkKafkaConsumer<>("keyed_click", schema, properties));

        SingleOutputStreamOperator<Click> sum = new KeyedClickTransformer(stream).perform();

        sum.print();

        env.execute(("ProcessingTime processing example"));
    }

}
