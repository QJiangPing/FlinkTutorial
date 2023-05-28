package org.example.sinkdemo;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Event;

import java.util.ArrayList;

public class SinkToMySql {
    public static void main(String[] args) throws Exception{
        //1获取执行环境execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",2000L));
        events.add(new Event("Alice","./login?id=1000",3000L));
        events.add(new Event("Bob","./login?id=1",3300L));
        events.add(new Event("Alice","./login?id=5",3200L));
        events.add(new Event("Bob","./home",3500L));
        events.add(new Event("Bob","./login?id=2",3800L));
        events.add(new Event("Bob","./login?id=3",4200L));
        DataStreamSource<Event> streamSource = env.fromCollection(events);

        //3sink端，输出到mysql

        //JdbcSink.sink(参数1，参数2，参数3)
        //参数1：sql语句（insert或update语句）
        //参数2：JdbcStatementBuilder，对sql语句做一些操作。这个接口是单一accept方法，所以可以用lambda表达式，主要是对参数1中的sql语句进行操作
        //参数3：JdbcConnectionOptions对象，放一些jdbc的连接信息，用builder模式创建。
        streamSource.addSink(JdbcSink.sink(
                "INSERT INTO click (user,url) values (?,?)",
                ((preparedStatement,event) -> {
                    preparedStatement.setString(1,event.getUser());
                    preparedStatement.setString(2,event.getUrl());
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/qjp")
                        //.withDriverName("com.mysql.jdbc.Driver")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("qjp199901")
                        .build()
        ));

        env.execute();
    }
}
