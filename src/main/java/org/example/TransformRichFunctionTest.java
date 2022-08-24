package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",2000L));
        events.add(new Event("Alice","./login?id=1000",3000L));
        DataStreamSource<Event> streamSource = env.fromCollection(events);

        streamSource.map(new MyRichMapper()).print();

        env.execute();

    }

    //实现一个自定义的富函数类
    public static class MyRichMapper extends RichMapFunction<Event,String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用："+getRuntimeContext().getIndexOfThisSubtask()+"号任务启动");
        }

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用："+getRuntimeContext().getIndexOfThisSubtask()+"号任务结束");
        }
    }
}
