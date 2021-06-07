package streamingdatapipelineexercise.examples.click.v6;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import streamingdatapipelineexercise.examples.click.shared.*;

import java.util.Properties;


/**
 *
 */

public class AvroClick {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String kafkaBoostrapServers = "localhost:9092";
        properties.setProperty("bootstrap.servers", kafkaBoostrapServers);
        String groupId = "AvroClickAllTop3";
        properties.setProperty("group.id", groupId);
        String kafkaTopic = "click_avro";

        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var expectedContent = "{\n" +
                "      \"type\": \"record\",\n" +
                "      \"name\": \"click\",\n" +
                "      \"fields\": [\n" +
                "          {\n" +
                "              \"name\": \"itemId\",\n" +
                "              \"type\": \"string\"\n" +
                "          },\n" +
                "          {\n" +
                "              \"name\": \"count\",\n" +
                "              \"type\": \"long\"\n" +
                "          },\n" +
                "          {\n" +
                "              \"name\": \"eventTime\",\n" +
                "              \"type\" : \"long\",\n" +
                "              \"logicalType\": \"timestamp-millis\"\n" +
                "          }\n" +
                "      ]\n" +
                "    }";
        Schema expectedSchema = new Schema.Parser().parse(expectedContent);

        var schema = ConfluentRegistryAvroDeserializationSchema.forGeneric(
                expectedSchema,
                "http://localhost:18081"
        );

        var stream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, schema, properties));

        stream.map((MapFunction<GenericRecord, Click>) value -> {
            Utf8 itemId = (Utf8) value.get("itemId");
            long count = (long) value.get("count");
            long eventTime = (long) value.get("eventTime");
            return new Click(
                    itemId.toString(), count, eventTime
            );
        }).print();

        env.execute("AvroClick");
    }
}
