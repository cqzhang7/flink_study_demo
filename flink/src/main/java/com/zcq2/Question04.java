package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.redis.RedisMessagePool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.servlet.RequestDispatcher;
import javax.swing.plaf.basic.BasicScrollPaneUI;
import java.util.*;

/**
 * @author cqzhang
 * @create 2020-12-12 8:47
 */
public class Question04 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> inputStream = env.readTextFile("flink/input").setParallelism(1);
        HashSet<String> wordlist = new HashSet<>();
        FlatMapOperator<String, String> stringSingleOutputStreamOperator = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(string);
                }
            }
        });

        FilterOperator<String> filter = stringSingleOutputStreamOperator.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                boolean res = !wordlist.contains(value);
                wordlist.add(value);
                return res;
            }
        });
        filter.print();

    }

}
