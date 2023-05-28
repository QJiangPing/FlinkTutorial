package org.example.cepdemo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.LoginEvent;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author qjp
 */
public class LoginFailDetectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.获取登录的数据流
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "192.168.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.10", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                        new LoginEvent("user_1", "192.168.1.29", "success", 9000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 10000L)
                )
                //定义水位线，设置可以允许延迟3秒
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));


        //2.定义模式：连续3次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                //第一次登录失败
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.eventType);
                    }
                })
                //第二次登录失败
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.eventType);
                    }
                })
                //第三次登录失败
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.eventType);
                    }
                });

        //3.将模式应用到数据流上，检测复杂事件
        //CEP.pattern(DataStream<T> input, Pattern<T, ?> pattern)
        //参数1：应用到哪个流处理上，参数2：哪个模式
        //通过user_id进行分组
        KeyedStream<LoginEvent, String> keyedStream = loginEventStream.keyBy(data -> data.getUserId());
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //4.将检测到的复杂事件提取出来，进行处理，得到报警信息数据
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                //提取复杂事件中的三次登录失败事件
                LoginEvent first = map.get("first").get(0);
                LoginEvent second = map.get("second").get(0);
                LoginEvent third = map.get("third").get(0);
                return first.getUserId() + "连续三次登录失败！登录ip/时间分别为：" +
                        first.getIpAddress() + "/" +first.getTimestamp() +", " +
                        second.getIpAddress() + "/" +second.getTimestamp() +", " +
                        third.getIpAddress() + "/" +third.getTimestamp() +", ";
            }
        }).print();

        env.execute();
    }
}
