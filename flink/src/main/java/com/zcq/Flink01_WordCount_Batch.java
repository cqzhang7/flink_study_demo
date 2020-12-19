package com.zcq;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author cqzhang
 * @create 2020-12-09 14:25
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据，批处理 from文件
        DataSource<String> input = executionEnvironment.readTextFile("flink/input");
        //3.flatmap压平处理并转换为元祖
        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = input.flatMap(new MyFlatMapper());

        //5.合并
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.aggregate(Aggregations.SUM,1);

        //6.打印输出
        sum.print();


    }
//    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
//
//        @Override
//        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//            String[] words = value.split(" ");
//            for (String word : words) {
//                out.collect(new Tuple2<String, Integer>(word,1));
//            }
//        }
//    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
