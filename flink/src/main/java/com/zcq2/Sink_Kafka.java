package com.zcq2;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author cqzhang
 * @create 2020-12-12 0:04
 */
public class Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");

        socketDataStream.addSink(new FlinkKafkaProducer011<String>("test",
                new SimpleStringSchema(),
                properties));

        env.execute("");
    }
}
