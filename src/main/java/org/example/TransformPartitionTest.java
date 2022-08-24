package org.example;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",2000L));
        events.add(new Event("Alice","./login?id=1000",3000L));
        events.add(new Event("Bob","./login?id=1",3200L));
        events.add(new Event("Alice","./login?id=5",3300L));
        events.add(new Event("Bob","./home",3500L));
        events.add(new Event("Bob","./login?id=2",3800L));
        events.add(new Event("Bob","./login?id=3",4200L));
        DataStreamSource<Event> streamSource = env.fromCollection(events);

        //1:随机分区
        streamSource.shuffle().print().setParallelism(4);

        //2:轮询分区:rebalance
        streamSource.rebalance().print().setParallelism(4);

        //3:rescale重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++){
                    // 将奇数偶数分别发送到0号和1号并行分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
                .rescale() //如果这里用rebalance()轮训，就是奇数在剩下的4个分区轮训输出，偶数也在剩下四个分区中轮训输出
                .print()
                .setParallelism(4);


        //4 广播,将到达的每一条数据全部广播到每一个分区，相当于一条数据，输出4次
        streamSource.broadcast().print().setParallelism(4);

        //5 全局,将所有的数据全部都分配到一个分区里
        streamSource.global().print().setParallelism(4);//这里的效果就会变成不管这里分区多少，都分到一个分区输出

        //6 自定义重分区
        env.fromElements(1,2,3,4,5,6,7,8)
                .partitionCustom(new Partitioner<Integer>() {
                                     @Override
                                     public int partition(Integer key, int numPartitions) {
                                         return key % 2;
                                     }
                                 }, new KeySelector<Integer, Integer>() {
                                     @Override
                                     public Integer getKey(Integer value) throws Exception {
                                         return value;
                                     }
                                 }
                )
                .print().setParallelism(4);

        env.execute();
    }
}
