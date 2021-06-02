package streamingdatapipelineexercise.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {
//  Run `nc -lk 9999` from terminal before running main method.
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env
                .socketTextStream("localhost", 9999);
        DataStream<Tuple2<String, Integer>> dataStream = new WordCountTransformer(dataStreamSource).perform();
        dataStream.print();
        env.execute("WordCount");
    }

}
