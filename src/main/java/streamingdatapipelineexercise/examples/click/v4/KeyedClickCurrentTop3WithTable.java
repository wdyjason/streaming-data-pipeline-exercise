package streamingdatapipelineexercise.examples.click.v4;

import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import streamingdatapipelineexercise.examples.click.shared.Config;
import streamingdatapipelineexercise.examples.click.shared.KeyedClickTransformer;
import streamingdatapipelineexercise.examples.click.shared.KeyedClickDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import streamingdatapipelineexercise.examples.click.shared.RawClick;

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

public class KeyedClickCurrentTop3WithTable {
    public static void main(String[] args) throws Exception {
        new KeyedClickCurrentTop3WithTable().execute();
    }

    public void execute() throws Exception {
        Properties properties = new Properties();
        String kafkaBoostrapServers = Config.KAFKA_BOOTSTRAP_SERVERS;
        properties.setProperty("bootstrap.servers", kafkaBoostrapServers);
        String groupId = "KeyedClickTop3";
        properties.setProperty("group.id", groupId);
        String kafkaTopic = "keyed_click";

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var schema = new KeyedClickDeserializationSchema();
        var stream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, schema, properties));

        var windowedClickStream = new KeyedClickTransformer(stream).perform();

        stream
                .keyBy(RawClick::getItemId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new KeyedClickTransformer.MyReduceFunction(), new KeyedClickTransformer.MyProcessWindowFunction());

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

        String jdbcURL = Config.JDBC_URL;
        String username = Config.JDBC_USERNAME;
        String password = Config.JDBC_PASSWORD;
        String dbTableName = "keyed_click_v4";

        String statement = createTableStatement +
                " WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = '" + jdbcURL + "',\n" +
                "   'table-name' = '" + dbTableName + "',\n" +
                "   'username' = '" + username + "',\n" +
                "   'password' = '" + password + "'\n" +
                ")";
        tableEnv.executeSql(statement);

        table.executeInsert(tablePath);
        env.execute("Click v3 processing");
    }
}
