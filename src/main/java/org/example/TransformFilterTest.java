package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TransformFilterTest {
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

        //3filter转换，过滤出用户名为zhangsan的点击事件
        //方式1，使用自定义类，传入实现FilterFunction接口的类对象
        SingleOutputStreamOperator<Event> result1 = streamSource.filter(new MyFilter());

        //方式2，使用匿名类，实现FilterFunction接口
        SingleOutputStreamOperator<Event> result2 = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("zhangsan");
            }
        });

        //方式3，使用lambda表达式
        SingleOutputStreamOperator<Event> result3 = streamSource.filter(data -> data.user.equals("zhangsan"));

        //4将计算结果的输出位置sink
        //result1.print();
        //result2.print();
        result3.print();

        //5触发程序执行execute
        env.execute();

    }


    //自定义类，实现FilterFunction接口
    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("zhangsan");
        }
    }

}
