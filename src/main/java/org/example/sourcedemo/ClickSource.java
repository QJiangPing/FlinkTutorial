package org.example.sourcedemo;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.Event;

import java.util.Calendar;
import java.util.Random;

//自定义数据源
public class ClickSource implements SourceFunction<Event> {
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
