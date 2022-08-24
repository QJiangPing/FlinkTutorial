package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.ArrayList;

public class TransformReduceTest {
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

        //用reduce实现最大访问量输出

        //1.统计每个用户的访问量
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = streamSource.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        //2.选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result1 = clickByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        //reduce 使用lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Long>> result2
                = clickByUser.keyBy(data -> "key").reduce((value1, value2) -> value1.f1 > value2.f1 ? value1 : value2);
        //打印输出
        result1.print();
        result2.print();

        env.execute();
    }
}
