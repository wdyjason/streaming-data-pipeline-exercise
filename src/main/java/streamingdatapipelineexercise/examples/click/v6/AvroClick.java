package streamingdatapipelineexercise.examples.click.v6;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.avro.registry.confluent.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import streamingdatapipelineexercise.examples.click.shared.*;

import java.util.Properties;


/**
 *
 */

public class AvroClick {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String kafkaBoostrapServers = Config.KAFKA_BOOTSTRAP_SERVERS;
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
                Config.SCHEMA_REGISTRY_SERVER
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
