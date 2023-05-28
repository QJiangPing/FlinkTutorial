package org.example.cepdemo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.OrderEvent;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author qjp
 */
public class OrderTimeoutDetectDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.获取数据流
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 600 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 1200 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

        //2.1 定义模式：15分钟内完成付款的订单
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.eventType);
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.eventType);
                    }
                })
                .within(Time.minutes(15));


        //2.2 将模式应用到订单数据流上,需要对订单进行key by
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(data -> data.getOrderId()), pattern);

        //2.3 定义一个侧输出流
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        //2.4 将完全匹配和超时匹配部分的复杂事件提取出来，进行处理
        SingleOutputStreamOperator<String> result = patternStream.process(new OrderPayMatch());

        //3.输出
        //直接将result用print输出的话，是只会将完全匹配的结果数据，超时的数据得获取到侧输出流后print才可以打印超时的数据
        result.print();
        result.getSideOutput(timeoutTag).print();
        env.execute();
    }

    private static class OrderPayMatch extends PatternProcessFunction<OrderEvent,String> implements TimedOutPartialMatchHandler<OrderEvent> {
        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {
            //处理完全匹配的数据
            OrderEvent payEvent = map.get("pay").get(0);
            collector.collect("用户"+payEvent.getUserId()+"订单"+payEvent.getOrderId()+"已支付");
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {
            //处理超时的数据
            OrderEvent createEvent = map.get("create").get(0);
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
            context.output(timeoutTag,"用户"+createEvent.getUserId()+"订单"+createEvent.getOrderId()+"已超时！！！！！");
        }
    }
}
