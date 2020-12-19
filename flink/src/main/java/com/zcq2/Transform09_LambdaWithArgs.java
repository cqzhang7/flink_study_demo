package com.zcq2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cqzhang
 * @create 2020-12-11 21:17
 */
public class Transform09_LambdaWithArgs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = executionEnvironment.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> filterStream = socketTextStream.filter(new MyFilter("hahha"));
        filterStream.print();
        executionEnvironment.execute();


    }
    public static class MyFilter implements FilterFunction<String> {
        private String keyWord;

        public MyFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(String value) throws Exception {
            return value.contains(keyWord);
        }
    }
}
