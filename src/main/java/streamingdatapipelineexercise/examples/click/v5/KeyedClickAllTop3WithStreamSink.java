package streamingdatapipelineexercise.examples.click.v5;

import streamingdatapipelineexercise.examples.click.shared.KeyedClickByTableTransformer;
import streamingdatapipelineexercise.examples.click.shared.KeyedClickDeserializationSchema;
import streamingdatapipelineexercise.examples.click.shared.WindowClickRecord;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


/**
 * Create the table first in postgresql
 *
 * CREATE TABLE keyed_click_v5 (
 *     itemId VARCHAR,
 *     "count" BIGINT,
 *     startTime timestamp,
 *     endTime timestamp,
 *     PRIMARY KEY(itemId, startTime)
 * )
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

public class KeyedClickAllTop3WithStreamSink {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String kafkaBoostrapServers = "localhost:9092";
        properties.setProperty("bootstrap.servers", kafkaBoostrapServers);
        String groupId = "KeyedClickTop3";
        properties.setProperty("group.id", groupId);
        String kafkaTopic = "keyed_click";

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var schema = new KeyedClickDeserializationSchema();
        var stream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, schema, properties));

        var windowedClickStream = new KeyedClickByTableTransformer(stream).perform();

        windowedClickStream.addSink(buildDatabaseSink(
                "jdbc:postgresql://localhost:5432/database",
                "postgres",
                "postgres"));

        env.execute("Click v3 processing");
    }

    private static SinkFunction<WindowClickRecord> buildDatabaseSink(String jdbcURL, String username, String password) {
        String dbTableName = "keyed_click_v5";
        return JdbcSink.sink(
                "INSERT INTO " + dbTableName + " (itemId, \"count\", startTime, endTime) values (?, ?, ?, ?)\n" +
                        "ON conflict(itemId, startTime) DO\n" +
                        "UPDATE\n" +
                        "SET \"count\" = ?, endTime = ?",
                (preparedStatement, click) -> {
                    preparedStatement.setString(1, click.getItemId());
                    preparedStatement.setLong(2, click.getCount());
                    preparedStatement.setTimestamp(3, click.getStartTime());
                    preparedStatement.setTimestamp(4, click.getEndTime());

                    preparedStatement.setLong(5, click.getCount());
                    preparedStatement.setTimestamp(6, click.getEndTime());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcURL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );
    }

}
