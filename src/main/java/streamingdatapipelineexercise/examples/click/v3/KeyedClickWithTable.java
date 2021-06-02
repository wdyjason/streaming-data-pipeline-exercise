package streamingdatapipelineexercise.examples.click.v3;

import streamingdatapipelineexercise.examples.click.shared.ClickRecord;
import streamingdatapipelineexercise.examples.click.shared.KeyedClickDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;


/**
 * Create the table first in postgresql
 *
 * CREATE TABLE keyed_click_v3 (
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

public class KeyedClickWithTable {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String kafkaBoostrapServers = "localhost:9092";
        properties.setProperty("bootstrap.servers", kafkaBoostrapServers);
        String groupId = "KeyedClick";
        properties.setProperty("group.id", groupId);
        String kafkaTopic = "keyed_click";

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var schema = new KeyedClickDeserializationSchema();
        var stream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, schema, properties));

        SingleOutputStreamOperator<ClickRecord> sum = new KeyedClickByTableTransformer(stream).perform();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(sum);

        String tablePath = "keyed_click";

        String createTableStatement = "CREATE TABLE " + tablePath + " (\n" +
                "  itemId VARCHAR PRIMARY KEY,\n" +
                "  `count` BIGINT,\n" +
                "  `timestamp` TIMESTAMP\n" +
                ")";

        String jdbcURL = "jdbc:postgresql://localhost:5432/database";
        String dbTableName = "keyed_click_v3";
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
        env.execute(("Click v3 processing"));
    }
}
