package org.example.sinkdemo;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.example.Event;

import java.util.concurrent.TimeUnit;

public class SinkToFile {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./login?id=1000", 3000L),
                new Event("Bob", "./login?id=1", 3200L),
                new Event("Alice", "./login?id=5", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./login?id=2", 3800L),
                new Event("Bob", "./login?id=3", 4200L));

        //路径
        Path path = new Path("./output");
        //编码格式
        Encoder<String> stringEncoder = new SimpleStringEncoder<String>("UTF-8");
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(path, stringEncoder)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                //单位：字节，这里为1G字节
                                .withMaxPartSize(1024 * 1024 * 1024)
                                //隔多久滚动一次，也就是要隔多久开启一个文件
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                //隔多久没有数据到来，我就认为当前文件已经结束了，归档保存，接下来的数据写入下一个
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .build()
                )
                .build();

        stream.map(data -> data.toString()).addSink(streamingFileSink);

        //不要忘了execute执行起来
        env.execute();
    }
}
