package streamingdatapipelineexercise.examples.click.v1;

import streamingdatapipelineexercise.examples.click.shared.Config;
import streamingdatapipelineexercise.examples.click.shared.RawClick;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


/**
 * Create the topic (optional):
 * kafka-topics --create --topic no_key_click --bootstrap-server kafka:19092
 *
 * Produce message with
 * kafka-console-producer --topic no_key_click --broker-list kafka:19092
 *
 * And messages like:
 * 100:1
 * 100:1
 * 100:2
 * 101:1
 */

public class NoKeyClick {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "NoKeyClick");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("no_key_click", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<RawClick> sum = new NoKeyClickTransformer(stream).perform();

        sum.print();

        env.execute(("NoKeyClick processing"));
    }

}
