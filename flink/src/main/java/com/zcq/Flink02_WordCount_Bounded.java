package com.zcq;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author cqzhang
 * @create 2020-12-09 19:33
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1.创建程序运行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件创建数据流
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("flink/input");

        //3.数据压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new MyFlatMapper1());

        //4.数据分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = tuple2SingleOutputStreamOperator.keyBy(0);

        //5.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2TupleKeyedStream.sum(1);
        //6.打印数据
        sum.print();
        //7.启动任务
        executionEnvironment.execute("test");

    }
    public static class MyFlatMapper1 implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
