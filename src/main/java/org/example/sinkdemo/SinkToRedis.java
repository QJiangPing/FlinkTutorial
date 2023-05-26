package org.example.sinkdemo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.example.Event;

import java.util.Calendar;
import java.util.Random;


/**
 * @author qjp
 */
public class SinkToRedis {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new ClickSource());


        //参数1：创建一个jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(1234)
                .setPassword("password")
                .build();

        //参数2：创建自定义类实现RedisMapper接口
        MyRedisMapper redisMapper = new MyRedisMapper();

        //sink端，输出到redis
        source.addSink(new RedisSink<>(config,redisMapper));


        //不要忘了execute执行起来
        env.execute();
    }

    //自定义数据源
    private static class ClickSource implements SourceFunction<Event> {
        //声明一个标志位，用来控制数据的生成
        private Boolean running = true;
        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            //随机生成数据
            Random random = new Random();
            //定义两个选取的数据集
            String[] users = {"Mary", "Alice", "Bob", "Cary", "Joly"};
            String[] urls = {"./home","./cart","./fav","./prod?id=100","./prod?id=10","./fav?id=1"};
            //循环生成随机数据
            while (running){
                String user = users[random.nextInt(users.length)];
                String url = urls[random.nextInt(urls.length)];
                long timeInMillis = Calendar.getInstance().getTimeInMillis();
                //sourceContext的collect方法向下发送
                sourceContext.collect(new Event(user,url,timeInMillis));

                //每隔一秒生成一条数据
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            //想要结束，只需要将标志位变成false
            running = false;
        }
    }

    public static class MyRedisMapper implements RedisMapper<Event> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            //返回当前一个当前redis操作命令的描述,
            //参数1：命令，参数2：哈希表名
            return new RedisCommandDescription(RedisCommand.HSET,"click");
        }

        @Override
        public String getKeyFromData(Event data) {
            //定义redis的key
            return data.user;
        }

        @Override
        public String getValueFromData(Event data) {
            //定义redis的value
            return data.url;
        }
    }
}
