package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TransformMapTest {
    public static void main(String[] args) throws Exception{
        //1获取执行环境execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("zhangsan","./home",1000L));
        events.add(new Event("lisi","./cart",2000L));
        events.add(new Event("wangwu","./login",3000L));
        DataStreamSource<Event> streamSource = env.fromCollection(events);

        //3map转换，将user对象只要里面的名字
        //方式1，使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = streamSource.map(new MyMapper());

        //方式2，使用匿名类，实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = streamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });

        //方式2，使用lambda表达式
        SingleOutputStreamOperator<String> result3 = streamSource.map(data -> data.user);

        //4将计算结果的输出位置sink
        //result1.print();
        //result2.print();
        result3.print();

        //5触发程序执行execute
        env.execute();

    }

    public static class MyMapper implements MapFunction<Event,String>{

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
