package com.zcq2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Set;


/**
 * @author cqzhang
 * @create 2020-12-12 10:37
 */
public class Q4redis {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSource<String> inputStream = env.readTextFile("flink/input").setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(string);
                }
            }
        });

        SingleOutputStreamOperator<String> filter = stringSingleOutputStreamOperator.filter(new MyFilter());
        filter.print();


    }
    public static class MyFilter extends RichFilterFunction<String>{
        private Jedis jedis;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            jedis = new Jedis("hadoop102", 6379);
        }

        @Override
        public boolean filter(String value) throws Exception {
            Set<String> filterRequest = jedis.smembers("filterRequest");
            boolean res = filterRequest.contains(value);
            jedis.sadd("filterRequest",value);
            return res;
        }

        @Override
        public void close() throws Exception {
            super.close();
            jedis.close();
        }
    }
}
