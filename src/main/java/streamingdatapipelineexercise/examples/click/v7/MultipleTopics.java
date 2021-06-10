package streamingdatapipelineexercise.examples.click.v7;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import streamingdatapipelineexercise.examples.click.shared.*;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 *
 */

public class MultipleTopics {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String kafkaBoostrapServers = Config.KAFKA_BOOTSTRAP_SERVERS;
        properties.setProperty("bootstrap.servers", kafkaBoostrapServers);
        String groupId = "MultipleTopics";
        properties.setProperty("group.id", groupId);

        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Click> eventStreamOperator = StreamBuilder.getClickStreamOperator(properties, "click_avro", env);

        SingleOutputStreamOperator<WindowClickRecord> clickWindowStream = eventStreamOperator
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .keyBy(Click::getItemId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(
                        (ReduceFunction<Click>) (e1, e2) -> new Click(
                                e1.getItemId(),
                                e1.getCount() + e2.getCount(),
                                Math.max(e1.getTimestamp(), e2.getTimestamp())),
                        new ProcessWindowFunction<>() {
                            @Override
                            public void process(String key, Context context, Iterable<Click> elements, Collector<WindowClickRecord> out) {
                                var record = elements.iterator().next();
                                TimeWindow window = context.window();
                                Timestamp from = Timestamp.from(Instant.ofEpochMilli(window.getStart()));
                                Timestamp to = Timestamp.from(Instant.ofEpochMilli(window.getEnd()));
                                out.collect(new WindowClickRecord(record.getItemId(), record.getCount(), from, to));
                            }
                        }
                );

        Table topNTable = tableEnv.fromDataStream(clickWindowStream)
                .orderBy(
                        $("endTime").desc(),
                        $("count").desc()
                ).limit(3);

        var itemStream = StreamBuilder
                .getItemStreamOperator(properties, "item_v1", env)
                .keyBy(Item::getItemId)
                .reduce((ReduceFunction<Item>) (value1, value2) -> value2);

        itemStream.print("itemStream");
        clickWindowStream.print("clickWindowStream");

        Table itemTable = tableEnv.fromDataStream(
                itemStream,
                Schema.newBuilder().primaryKey("itemId").build()
        );

        topNTable.printSchema();
        itemTable.printSchema();

        Table right = itemTable.renameColumns($("itemId").as("item_itemId"));

        right.printSchema();

        tableEnv.toChangelogStream(topNTable).print("topNTable-toChangelogStream");
        tableEnv.toChangelogStream(right).print("itemTable-toChangelogStream");

        Table joinedTable = topNTable
                .join(right)
                .where($("itemId").isEqual($("item_itemId")))
                .select(
                        $("itemId"),
                        $("count"),
                        $("startTime"),
                        $("endTime"),
                        $("description")
                );

        joinedTable.printSchema();

        tableEnv.toChangelogStream(joinedTable).print("joinedTable-toChangelogStream");

        String tablePath = "keyed_click";

        String createTableStatement = "CREATE TABLE " + tablePath + " (\n" +
                "  itemId VARCHAR PRIMARY KEY,\n" +
                "  `count` BIGINT,\n" +
                "  startTime TIMESTAMP,\n" +
                "  endTime TIMESTAMP,\n" +
                "  description VARCHAR\n" +
                ")";

        String jdbcURL = "jdbc:postgresql://localhost:5432/database";
        String dbTableName = "click_v7";
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

        joinedTable.executeInsert(tablePath);

        env.execute("MultipleTopics");
    }
}
