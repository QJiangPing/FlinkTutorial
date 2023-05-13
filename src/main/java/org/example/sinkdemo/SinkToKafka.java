package org.example.sinkdemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.example.Event;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SinkToKafka {
    public static void main(String[] args) throws Exception{
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //2读取数据源source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.service","hadoop102:9092");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        //3处理(输入，输出)
        kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Event(fields[0],fields[1],Long.valueOf(fields[2])).toString();
            }
        });

        //4Sink输出
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>("hadoop102:9092", "event", new SimpleStringSchema());
        kafkaStream.addSink(flinkKafkaProducer);

        //5不要忘了execute执行起来
        env.execute();
    }
}
