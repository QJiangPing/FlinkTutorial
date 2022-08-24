package org.example;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TransformSimpleAgg {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",2000L));
        events.add(new Event("Alice","./login?id=1000",3000L));
        events.add(new Event("Bob","./login?id=1",3300L));
        events.add(new Event("Alice","./login?id=5",3200L));
        events.add(new Event("Bob","./home",3500L));
        events.add(new Event("Bob","./login?id=2",3800L));
        events.add(new Event("Bob","./login?id=3",4200L));
        DataStreamSource<Event> streamSource = env.fromCollection(events);

        //按键分组后进行聚合，选取当前每个用户，所有访问数据里边，最近一次的访问数据

        //只对max提取的字段进行取最大值，其他字段以第一次到达的数据为准
        streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("通过max:");

        //直接把当前最大时间戳的那条完整的Event保存下来，输出
        streamSource.keyBy(data -> data.user).maxBy("timestamp").print("通过maxBy");

        env.execute();

    }
}
