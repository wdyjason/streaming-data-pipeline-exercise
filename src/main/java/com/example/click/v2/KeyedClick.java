package com.example.click.v2;

import com.example.click.shared.Click;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;


/**
 * Create the table first in postgresql
 *
 * CREATE TABLE keyed_click (
 *     itemId VARCHAR PRIMARY KEY,
 *     "count" BIGINT,
 *     "timestamp" timestamp
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

public class KeyedClick {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "KeyedClick");
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var schema = new KeyedClickDeserializationSchema();
        String kafkaTopic = "keyed_click";
        var stream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, schema, properties));

        SingleOutputStreamOperator<Click> sum = new KeyedClickTransformer(stream).perform();

        sum.print();

        sum.addSink(buildDatabaseSink(
                "jdbc:postgresql://localhost:5432/database",
                "postgres",
                "postgres"));

        env.execute(("KeyedClick processing"));
    }

    private static SinkFunction<Click> buildDatabaseSink(String jdbcURL, String username, String password) {
        String dbTableName = "keyed_click";
        return JdbcSink.sink(
                "INSERT INTO " + dbTableName + " (itemId, \"count\", \"timestamp\") values (?, ?, ?)\n" +
                        "ON conflict(itemId) DO\n" +
                        "UPDATE\n" +
                        "SET \"count\" = ?, \"timestamp\" = ?",
                (preparedStatement, click) -> {
                    preparedStatement.setString(1, click.getItemId());
                    preparedStatement.setLong(2, click.getCount());
                    Timestamp timestamp = Timestamp.from(Instant.ofEpochMilli(click.getTimestamp()));
                    preparedStatement.setTimestamp(3, timestamp);
                    preparedStatement.setLong(4, click.getCount());
                    preparedStatement.setTimestamp(5, timestamp);
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
