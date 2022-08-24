package org.example.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception{
        //1一个流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        //3转化
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneTupleGroup = wordAndOneTuple.keyBy(data -> data.f0);//用lambda表达式取第一个字段作为key

        //5求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneTupleGroup.sum(1);

        //6打印
        sum.print();

        //7启动执行
        env.execute();

    }
}
