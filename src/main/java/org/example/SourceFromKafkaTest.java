package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class SourceFromKafkaTest {
    public static void main(String[] args) throws Exception{
        //1获取执行环境execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2读取数据源source
        //
        Properties properties = new Properties();
        //
        properties.setProperty("bootstrap.service","hadoop102:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));


        //3定义给予数据的转换操作transformation

        //4顶级计算结果的输出位置sink
        kafkaStream.print();


        //5触发程序执行execute
        env.execute();
    }
}
