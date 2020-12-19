package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author cqzhang
 * @create 2020-12-14 8:44
 */
public class Kafka_WC_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer011<String>("test",
                new SimpleStringSchema(),
                properties));
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = kafkaStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = tuple2SingleOutputStreamOperator.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2TupleKeyedStream.sum(1);
        sum.addSink(new MyJdbcSink());


        env.execute("");
    }

    public static class MyJdbcSink extends RichSinkFunction<Tuple2<String, Integer>> {

        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        // open 主要是创建连接
        @Override
        public void open(Configuration parameters) throws Exception {

            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");

            // 创建预编译器，有占位符，可传入参数
            insertStmt = conn.prepareStatement("INSERT INTO wordcount (word, count) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE wordcount SET count = ? WHERE word = ?");
        }

        // 调用连接，执行sql
        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {

            // 执行更新语句，注意不要留super
            updateStmt.setString(1, value._1());
            updateStmt.setString(2, String.valueOf(value._2()));
            updateStmt.execute();

            // 如果刚才update语句没有更新，那么插入
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value._1());
                insertStmt.setString(2, String.valueOf(value._2()));
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }

}
