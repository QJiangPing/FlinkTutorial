package org.example.sinkdemo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.example.Event;
import org.example.sourcedemo.ClickSource;



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
