package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception{
        //1获取执行环境execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("zhangsan","./home",1000L));
        events.add(new Event("lisi","./cart",2000L));
        events.add(new Event("wangwu","./login?id=1000",3000L));
        events.add(new Event("六六","./login?id=1222",3000L));
        DataStreamSource<Event> streamSource = env.fromCollection(events);

        //方式1，使用一个实现了FlatMapFunction接口的自定义对象
        streamSource.flatMap(new MyFlatMap()).print();

        //方式2，使用匿名函数
        //将张三的用户名输出，将李四的url输出，将王五的用户名和时间输出,其他人过滤掉
        streamSource.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> out) throws Exception {
                if (event.user.equals("zhangsan")){
                    out.collect(event.user);
                }else if (event.user.equals("lisi")){
                    out.collect(event.url);
                } else if (event.user.equals("wangwu")) {
                    out.collect(event.user);
                    out.collect(event.timestamp.toString());//因为定义了泛型输出是string类型，所以这里要将时间戳转字符串
                }
            }
        }).print();

        //.returns(new TypeHint<String>() {});//这里当前的JVM会出现泛型擦除的情况，所以需要用returns方法给出对应的类型输出。

        //方式3，使用lambda表达式
        streamSource.flatMap(((Event event, Collector<String> out) -> {
            if (event.user.equals("zhangsan")){
                out.collect(event.user);
            }else if (event.user.equals("lisi")){
                out.collect(event.url);
            } else if (event.user.equals("wangwu")) {
                out.collect(event.user);
                out.collect(event.timestamp.toString());//因为Collector<String>定义了泛型输出是string类型，所以这里要将时间戳转字符串
            }
        })).returns(new TypeHint<String>() {})//这里当前的JVM会出现泛型擦除的情况，所以需要用returns方法给出对应的类型输出。
                .print();

        //5触发程序执行execute
        env.execute();

    }

    //自定义类实现FlatMapFunction
    public static class MyFlatMap implements FlatMapFunction<Event,String>{
        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            out.collect(event.user);
            out.collect(event.url);
            out.collect(event.timestamp.toString());
        }
    }
}
