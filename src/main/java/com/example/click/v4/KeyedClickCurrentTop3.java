package com.example.click.v4;

import com.example.click.shared.ClickRecord;
import com.example.click.shared.KeyedClickDeserializationSchema;
import com.example.click.shared.WindowClickRecord;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;


/**
 * Create the table first in postgresql
 *
 * CREATE TABLE keyed_click_v4 (
 *     itemId VARCHAR PRIMARY KEY,
 *     "count" BIGINT,
 *     startTime timestamp,
 *     endTime timestamp
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

public class KeyedClickCurrentTop3 {
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

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        var top = 3;
        Table table = tableEnv.fromDataStream(windowedClickStream)
                .orderBy(
                        $("endTime").desc(),
                        $("count").desc()
                ).limit(top);

        String tablePath = "keyed_click";

        String createTableStatement = "CREATE TABLE " + tablePath + " (\n" +
                "  itemId VARCHAR PRIMARY KEY,\n" +
                "  `count` BIGINT,\n" +
                "  startTime TIMESTAMP,\n" +
                "  endTime TIMESTAMP\n" +
                ")";

        String jdbcURL = "jdbc:postgresql://localhost:5432/database";
        String dbTableName = "keyed_click_v4";
        String username = "postgres";
        String password = "'postgres'";

        String statement = createTableStatement +
                " WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = '" + jdbcURL + "',\n" +
                "   'table-name' = '" + dbTableName + "',\n" +
                "   'username' = '" + username + "',\n" +
                "   'password' = " + password + "\n" +
                ")";
        tableEnv.executeSql(statement);

        table.executeInsert(tablePath);
        env.execute("Click v3 processing");
    }
}
