package streamingdatapipelineexercise.examples.click.homework;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import streamingdatapipelineexercise.examples.click.shared.*;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

//create table homework
//        (
//        itemid      varchar not null
//        constraint homework_pk
//        primary key,
//        count       bigint,
//        starttime   timestamp,
//        endtime     timestamp,
//        description varchar
//        );

public class HomeWork {

    private Properties properties;

    private StreamExecutionEnvironment streamEnv;

    private StreamTableEnvironment tableEnv;

    private final String tablePath = "keyed_click";

    public static void main(String[] args) {
        new HomeWork().execute();
    }


    public void execute () {
        initConfig();


        Table topNTable = getTopNTable();
        Table itemTable = getItemTable();

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
        joinedTable.printSchema();

        tableEnv.toChangelogStream(joinedTable).print("joinedTable-toChangelogStream");
        persistTable(joinedTable);

    }

    private void persistTable(Table joinedTable) {

        String createTableStatement = "CREATE TABLE " + tablePath + " (\n" +
                "  itemId VARCHAR PRIMARY KEY,\n" +
                "  `count` BIGINT,\n" +
                "  startTime TIMESTAMP,\n" +
                "  endTime TIMESTAMP,\n" +
                "  description VARCHAR\n" +
                ")";

        String jdbcURL = Config.JDBC_URL;
        String username = Config.JDBC_USERNAME;
        String password = Config.JDBC_PASSWORD;
        String dbTableName = "homework";

        String statement = createTableStatement +
                " WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = '" + jdbcURL + "',\n" +
                "   'table-name' = '" + dbTableName + "',\n" +
                "   'username' = '" + username + "',\n" +
                "   'password' = '" + password + "'\n" +
                ")";

        tableEnv.executeSql(statement);
        joinedTable.executeInsert(tablePath);
    }

    private void initConfig() {
        properties = new Properties();
        properties.setProperty("bootstrap.servers",  Config.KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "MultipleTopics");

        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.enableCheckpointing(5000);

        tableEnv = StreamTableEnvironment.create(streamEnv);
    }


    public Table getTopNTable() {
        SingleOutputStreamOperator<Click> eventStreamOperator = StreamBuilder.getClickStreamOperator(properties, "click_avro", streamEnv);

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
                        new ClickWindowFunction()
                );

        clickWindowStream.print("clickWindowStream");

        return tableEnv.fromDataStream(clickWindowStream)
                .orderBy(
                        $("endTime").desc(),
                        $("count").desc()
                ).limit(3);
    }

    public Table getItemTable() {
        var itemStream = StreamBuilder
                .getItemStreamOperator(properties, "item_v1", streamEnv)
                .keyBy(Item::getItemId)
                .reduce((ReduceFunction<Item>) (value1, value2) -> value2);

        itemStream.print("itemStream");

        return tableEnv.fromDataStream(
                itemStream,
                Schema.newBuilder().primaryKey("itemId").build()
        );
    }

}
