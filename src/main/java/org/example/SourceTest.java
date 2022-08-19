package org.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SourceTest {
    public static void main(String[] args) throws Exception{
        //1获取执行环境execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2读取数据源source
        //方式1：从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");
        //方式2：从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(5);
        DataStreamSource<Integer> stream2 = env.fromCollection(nums);
        //方式3：从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("zhangsan","./home",1000L));
        events.add(new Event("lisi","./cart",2000L));
        events.add(new Event("wangwu","./login",3000L));
        DataStreamSource<Event> stream3 = env.fromCollection(events);
        //方式4： 从socket文本流中读取
        env.socketTextStream("localhost",7777);

        //3定义给予数据的转换操作transformation

        //4顶级计算结果的输出位置sink
        stream1.print("from txt file");
        stream2.print("from numbers collection");
        stream3.print("from Object collection");

        //5触发程序执行execute
        env.execute();
    }
}
