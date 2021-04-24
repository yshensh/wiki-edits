package wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import java.util.Properties;

public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {
        // set execution parameters
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        // create sources for reading from external systems
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        // create DataStream of WikipediaEditEvent elements
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });

        // aggregate the sum of edited bytes for every five seconds
        DataStream<Tuple2<String, Long>> result = keyedEdits
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(WikipediaEditEvent event, Tuple2<String, Long> accumulator) {
                        accumulator.f0 = event.getUser();
                        accumulator.f1 += event.getByteDiff();
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> cur, Tuple2<String, Long> accumulator) {
                        return new Tuple2<>(cur.f0, cur.f1 + accumulator.f1);
                    }
                });


        // configuration for Kafka sink
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "wiki-result",       // target topic
                new SimpleStringSchema(),   // serialization schema
                properties                  // producer config
        );

        // write to Kafka
        result.map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> tuple) {
                return tuple.toString();
            }
        }).addSink(kafkaProducer);

        // print the stream to the console and start execution
        //result.print();

        see.execute();
    }
}