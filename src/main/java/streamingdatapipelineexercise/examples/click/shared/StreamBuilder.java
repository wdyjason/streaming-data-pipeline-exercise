package streamingdatapipelineexercise.examples.click.shared;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class StreamBuilder {
    public static SingleOutputStreamOperator<Click> getClickStreamOperator(Properties properties, String kafkaTopic, StreamExecutionEnvironment env) {
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

        return stream.map((MapFunction<GenericRecord, Click>) value -> {
            Utf8 itemId = (Utf8) value.get("itemId");
            long count = (long) value.get("count");
            long eventTime = (long) value.get("eventTime");
            return new Click(
                    itemId.toString(), count, eventTime
            );
        });
    }

    public static SingleOutputStreamOperator<Item> getItemStreamOperator(Properties properties, String kafkaTopic, StreamExecutionEnvironment env) {
        var expectedSchemaContent = "{\n" +
                "    \"type\": \"record\",\n" +
                "    \"name\": \"item\",\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"itemId\",\n" +
                "            \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"description\",\n" +
                "            \"type\": \"string\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        Schema expectedSchema = new Schema.Parser().parse(expectedSchemaContent);

        var schema = ConfluentRegistryAvroDeserializationSchema.forGeneric(
                expectedSchema,
                Config.SCHEMA_REGISTRY_SERVER
        );

        FlinkKafkaConsumer<GenericRecord> consumer = new FlinkKafkaConsumer<>(kafkaTopic, schema, properties);
        consumer.setStartFromEarliest();
        var stream = env
                .addSource(consumer);

        return stream.map((MapFunction<GenericRecord, Item>) value -> {
            Utf8 itemId = (Utf8) value.get("itemId");
            Utf8 description = (Utf8) value.get("description");
            return new Item(
                    itemId.toString(), description.toString()
            );
        });
    }

}
