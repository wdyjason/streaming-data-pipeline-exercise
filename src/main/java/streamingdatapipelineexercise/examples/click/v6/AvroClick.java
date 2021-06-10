package streamingdatapipelineexercise.examples.click.v6;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamingdatapipelineexercise.examples.click.shared.Click;
import streamingdatapipelineexercise.examples.click.shared.Config;
import streamingdatapipelineexercise.examples.click.shared.StreamBuilder;

import java.util.Properties;


/**
 *
 */

public class AvroClick {
    public static void main(String[] args) throws Exception {
        new AvroClick().execute();
    }

    private void execute() throws Exception {
        Properties properties = new Properties();
        String kafkaBoostrapServers = Config.KAFKA_BOOTSTRAP_SERVERS;
        properties.setProperty("bootstrap.servers", kafkaBoostrapServers);
        String groupId = "AvroClickAllTop3";
        properties.setProperty("group.id", groupId);
        String kafkaTopic = "click_avro";

        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Click> map = StreamBuilder.getClickStreamOperator(properties, kafkaTopic, env);

        env.execute("AvroClick");
    }
}
